package co.elastic.s3http;

import io.airlift.compress.zstd.ZstdOutputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnitTests {

    static final S3Indexer.Request REQ = new S3Indexer.Request("r", 1790872170000L, "n", "1.2.3.4", new AccessKeys.Key("cli", "AK", "s"),
        10, "aws-cli", "0123456789abcdef0123456789abcdef");

    static List<String> records(byte[] body) throws IOException {
        return records(body, true);
    }

    static List<String> records(byte[] body, boolean jsonFile) throws IOException {
        List<String> out = new ArrayList<>();
        try (ObjectDecoder.Records r = ObjectDecoder.open(body, jsonFile).records()) {
            for (String s; (s = r.next()) != null; ) {
                out.add(s);
            }
        }
        return out;
    }

    static byte[] compress(String compression, byte[] data) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (OutputStream out = switch (compression) {
            case "gzip" -> new GZIPOutputStream(bytes);
            case "zstd" -> new ZstdOutputStream(bytes);
            case "lz4" -> new LZ4FrameOutputStream(bytes);
            default -> bytes;
        }) {
            out.write(data);
        }
        return bytes.toByteArray();
    }

    @Test
    void accessKeysParse() {
        Map<String, AccessKeys.Key> k = AccessKeys.parse(List.of("dc1=AKIA123:s3cr/et+x", "AKIA456:other"));
        assertEquals("dc1", k.get("AKIA123").name());
        assertEquals("s3cr/et+x", k.get("AKIA123").secret());
        assertEquals("AKIA456", k.get("AKIA456").name());
        assertThrows(IllegalArgumentException.class, () -> AccessKeys.parse(List.of("nosecret")));
        assertThrows(IllegalArgumentException.class, () -> AccessKeys.parse(List.of("a b=AKIA1:x")));
        assertThrows(IllegalArgumentException.class, () -> AccessKeys.parse(List.of("AKIA1:x", "AKIA1:y")));
    }

    @Test
    void lineBecomesADocument() {
        ObjectKey key = ObjectKey.parse(List.of("b", "app", "year=2026", "service=cart", "part-1.ndjson.zst"));
        ObjectDecoder.Opened object = new ObjectDecoder.Opened(null, "zstd", "lines");
        Map<String, Object> src = S3Indexer.toSource("{\"msg\":\"hi\"}", 7, object, key, REQ);
        assertEquals("{\"msg\":\"hi\"}", src.get("message"));
        assertEquals("2026-10-01T16:29:30Z", src.get("@timestamp"));
        assertEquals(Map.of("module", "s3", "ingested", "2026-10-01T16:29:30Z"), src.get("event"));
        assertEquals(Map.of("type", "aws-cli"), src.get("agent"));
        @SuppressWarnings("unchecked")
        Map<String, Object> ns = (Map<String, Object>) src.get("s3");
        assertEquals("b", ns.get("bucket"));
        assertEquals("app/year=2026/service=cart/part-1.ndjson.zst", ns.get("key"));
        assertEquals(7, ns.get("line"));
        assertEquals("zstd", ns.get("compression"));
        assertEquals("lines", ns.get("format"));
        assertEquals(REQ.etag(), ns.get("etag"));
        assertEquals(Map.of("year", "2026", "service", "cart"), ns.get("partition"));
        assertEquals("cli", ns.get("access_key_name"));
        assertEquals("AK", ns.get("access_key_id"));
        @SuppressWarnings("unchecked")
        Map<String, Object> request = (Map<String, Object>) ns.get("request");
        assertNull(request.get("access_key_name"));
        assertEquals("2026-10-01T16:29:30Z", request.get("received"));
        assertEquals("aws-cli", request.get("user_agent"));
        assertEquals(10L, request.get("bytes"));
    }

    @Test
    void agentFromUserAgent() {
        assertEquals(Map.of("type", "aws-cli", "version", "2.17.0"), S3Indexer.agent("aws-cli/2.17.0 Python/3.11.8 Linux/6.8 exe/x86_64"));
        assertEquals(Map.of("type", "aws-sdk-java", "version", "1.12.x"), S3Indexer.agent("aws-sdk-java/1.12.x Linux/6.8 OpenJDK/21"));
        assertEquals(Map.of("type", "Mozilla", "version", "5.0"), S3Indexer.agent("Mozilla/5.0 (X11; Linux x86_64)"));
        assertEquals(Map.of("type", "app", "version", "1.0"), S3Indexer.agent("app/1.0;build=7"));
        assertEquals(Map.of("type", "app", "version", "1.0"), S3Indexer.agent(" app /1.0)"));
        assertEquals(Map.of("type", "aws-fluent-bit-plugin"), S3Indexer.agent("aws-fluent-bit-plugin"));
        assertEquals(Map.of("type", "rclone"), S3Indexer.agent("rclone (custom build)"));
        assertEquals(Map.of("type", "app"), S3Indexer.agent("app/"));
        assertNull(S3Indexer.agent(null));
        assertNull(S3Indexer.agent(""));
        assertNull(S3Indexer.agent("   "));
        assertNull(S3Indexer.agent("/1.0"));
    }

    @Test
    void noUserAgentNoAgent() {
        S3Indexer.Request req = new S3Indexer.Request("r", 1790872170000L, "n", "1.2.3.4", null, 10, null, null);
        ObjectKey key = ObjectKey.parse(List.of("b", "part-1.ndjson"));
        Map<String, Object> full = S3Indexer.toSource("{}", 1, new ObjectDecoder.Opened(null, null, "lines"), key, req);
        Map<String, Object> minimal = S3Indexer.minimalSource("{}", 1, key, req);
        Map<String, Object> error = S3Indexer.errorSource("bad", null, key, req, null);
        for (Map<String, Object> src : List.of(full, minimal, error)) {
            assertNull(src.get("agent"));
            @SuppressWarnings("unchecked")
            Map<String, Object> event = (Map<String, Object>) src.get("event");
            assertEquals("s3", event.get("module"));
        }
    }

    @Test
    void minimalMetadata() {
        ObjectKey key = ObjectKey.parse(List.of("b", "app", "service=cart", "part-1.ndjson"));
        Map<String, Object> src = S3Indexer.minimalSource("{\"msg\":\"hi\"}", 7, key, REQ);
        assertEquals(Map.of("bucket", "b", "key", "app/service=cart/part-1.ndjson", "line", 7, "access_key_name", "cli"),
            src.get("s3"));
        assertEquals(java.util.Set.of("@timestamp", "message", "s3", "agent", "event"), src.keySet());
        assertEquals(Map.of("module", "s3"), src.get("event"));
        assertEquals("minimal", S3HttpSettings.METADATA.get(Settings.builder().put("s3.metadata", "minimal").build()));
        assertThrows(IllegalArgumentException.class,
            () -> S3HttpSettings.METADATA.get(Settings.builder().put("s3.metadata", "none").build()));
    }

    @Test
    void targetPlaceholders() {
        S3Indexer indexer = new S3Indexer(null, null, Settings.builder().put("s3.target", "logs-{service}.{bucket}-{missing}")
            .put("xpack.security.enabled", false).build());
        ObjectKey key = ObjectKey.parse(List.of("My-Bucket", "x", "service=Cart:API", "f.ndjson"));
        assertEquals("logs-cart_api.my_bucket-unknown", indexer.resolveTarget(key));
        assertEquals("x_internal", S3Indexer.sanitize("_internal"));
        assertEquals("unknown", S3Indexer.sanitize(null));
        assertEquals(S3Indexer.MAX_NAME_PART, S3Indexer.sanitize("x".repeat(300)).length());
    }

    @Test
    void targetValidation() {
        S3HttpSettings.validateTarget("logs-app.{service}-default");
        S3HttpSettings.validateTarget("logs-s3.app-default");
        // the default is a stream the plugin's template (logs-s3.*-*) matches
        assertEquals("logs-s3.generic-default", S3HttpSettings.TARGET.get(Settings.EMPTY));
        S3HttpSettings.validateTarget("{bucket}");
        for (String bad : new String[] {"Logs-app", "logs app", "logs-{bad name}", "_logs", "", "logs/x", "logs-{}"}) {
            assertThrows(IllegalArgumentException.class, () -> S3HttpSettings.validateTarget(bad), bad);
        }
    }

    @Test
    void keyPartitions() {
        ObjectKey k = ObjectKey.parse(List.of("b", "dest", "year=2026", "month=10", "sourcetype=app:json", "events_1.ndjson"));
        assertEquals(Map.of("year", "2026", "month", "10", "sourcetype", "app:json"), k.partition());
        assertEquals("dest/year=2026/month=10/sourcetype=app:json/events_1.ndjson", k.key());
        // the file name is not a partition, and neither is a segment whose name isn't one
        assertEquals(Map.of(), ObjectKey.parse(List.of("b", "a=b.ndjson")).partition());
        assertEquals(Map.of(), ObjectKey.parse(List.of("b", "x y=1", "f")).partition());
    }

    @Test
    void docIdsAreStablePerObjectContentAndLine() {
        ObjectKey k = ObjectKey.parse(List.of("b", "p", "f.ndjson"));
        assertEquals(S3Indexer.docId(k, REQ, 7), S3Indexer.docId(k, REQ, 7));
        assertNotEquals(S3Indexer.docId(k, REQ, 7), S3Indexer.docId(k, REQ, 8));
        S3Indexer.Request other = new S3Indexer.Request("r2", 0, "n", null, null, 10, null, "ffffffffffffffffffffffffffffffff");
        assertNotEquals(S3Indexer.docId(k, REQ, 7), S3Indexer.docId(k, other, 7)); // new content under the same key is new documents
        assertEquals(20, S3Indexer.docId(k, REQ, 7).length());
    }

    @Test
    void everyCompressionAndFormatDecodes() throws IOException {
        String ndjson = "{\"a\":1}\n\n{\"a\":2}\r\n";
        String array = " [ {\"a\":1, \"s\":\"],[{\\\"\"},\n {\"tags\":\"x\",\"tags\":\"y\",\"n\":[1,[2]]} ,\"str\", 3 ]";
        for (String c : List.of("none", "gzip", "zstd", "lz4")) {
            byte[] body = compress(c, ndjson.getBytes(StandardCharsets.UTF_8));
            assertEquals(c, ObjectDecoder.sniffCompression(body));
            ObjectDecoder.Opened o = ObjectDecoder.open(body, true);
            assertEquals("lines", o.format());
            assertEquals(c, o.compression());
            assertEquals(List.of("{\"a\":1}", "{\"a\":2}"), records(body));

            body = compress(c, array.getBytes(StandardCharsets.UTF_8));
            assertEquals("json_array", ObjectDecoder.open(body, true).format());
            // each element exactly as written: repeated keys and brackets inside strings survive
            assertEquals(List.of("{\"a\":1, \"s\":\"],[{\\\"\"}", "{\"tags\":\"x\",\"tags\":\"y\",\"n\":[1,[2]]}", "\"str\"", "3"), records(body));
        }
        // raw text: a document per line, whatever it is
        assertEquals(List.of("Oct 14 12:00:01 host sshd[1]: ok", "second line"), records("Oct 14 12:00:01 host sshd[1]: ok\nsecond line".getBytes()));
        assertEquals(List.of(), records("[]".getBytes()));
        assertEquals(List.of(), records(new byte[0]));
    }

    /** Only a .json file is read as one array; text or ndjson that happens to start with '[' is lines, never cut short. */
    @Test
    void arrayModeIsExplicitAndRefusesTrailingData() throws IOException {
        byte[] text = "[INFO] a\n[INFO] b\n".getBytes();
        assertEquals(List.of("[INFO] a", "[INFO] b"), records(text, false));
        assertEquals("lines", ObjectDecoder.open(text, false).format());
        byte[] arraysPerLine = "[1,2]\n[3,4]\n".getBytes();
        assertEquals(List.of("[1,2]", "[3,4]"), records(arraysPerLine, false));
        assertThrows(IOException.class, () -> records(arraysPerLine, true)); // a .json file holding two arrays
        assertEquals(List.of("{\"a\":1}"), records("[{\"a\":1}]\n  \n".getBytes(), true));
        assertEquals(true, ObjectKey.parse(List.of("b", "x", "events_1.json.zst")).jsonFile());
        assertEquals(true, ObjectKey.parse(List.of("b", "x.JSON")).jsonFile());
        assertEquals(false, ObjectKey.parse(List.of("b", "x.ndjson.gz")).jsonFile());
        assertEquals(false, ObjectKey.parse(List.of("b", "x.log")).jsonFile());
    }

    /** A part that can't be read back fails the request (retried, parts kept); it is never mistaken for corrupt data. */
    @Test
    void unreadablePartIsNotADecodeError() throws IOException {
        byte[] first = compress("zstd", "{\"a\":1}\n".repeat(100).getBytes());
        for (boolean compressed : List.of(true, false)) {
            byte[] part1 = compressed ? java.util.Arrays.copyOf(first, first.length / 2) : "{\"a\":1}\n".getBytes();
            List<Multipart.Part> parts = List.of(new Multipart.Part(1, "x"), new Multipart.Part(2, "y"));
            java.io.InputStream in = Multipart.concat(parts, n -> {
                if (n == 1) {
                    return new BytesArray(part1);
                }
                throw new ObjectDecoder.PartUnavailable("part 2 is gone", null);
            });
            assertThrows(ObjectDecoder.PartUnavailable.class, () -> {
                try (ObjectDecoder.Records r = ObjectDecoder.open(in, 1000, false).records()) {
                    while (r.next() != null) {}
                }
            });
        }
    }

    @Test
    void decompressedSizeIsCapped() throws IOException {
        byte[] bomb = compress("zstd", "{\"a\":1}\n".repeat(200_000).getBytes()); // 1.6 MB from a few KB
        try (ObjectDecoder.Records r = ObjectDecoder.open(new BytesArray(bomb).streamInput(), bomb.length, false).records()) {
            // under the 1 GiB floor: read in full
            int n = 0;
            while (r.next() != null) {
                n++;
            }
            assertEquals(200_000, n);
        }
        ObjectDecoder.LineLimit limit = new ObjectDecoder.LineLimit(new BytesArray("a\nb\nc\n".getBytes()).streamInput(), 100, 4);
        assertThrows(IOException.class, () -> limit.readAllBytes());
    }

    @Test
    void lineNumbersCountBlankLines() throws IOException {
        try (ObjectDecoder.Records r = ObjectDecoder.open("{\"a\":1}\n\n{\"a\":2}\n".getBytes(), false).records()) {
            r.next();
            assertEquals(1, r.number());
            r.next();
            assertEquals(3, r.number());
            assertNull(r.next());
        }
    }

    @Test
    void corruptOrTruncatedObjects() throws Exception {
        // corrupt input is an IOException (on open or while reading), which becomes an error document, not a 500
        byte[][] garbage = {
            {(byte) 0x1f, (byte) 0x8b, 'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x'},
            {0x04, 0x22, 0x4d, 0x18, 'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x'},
            {0x28, (byte) 0xb5, 0x2f, (byte) 0xfd, 'G', 'A', 'R', 'B', 'A', 'G', 'E', 'G', 'A', 'R', 'B', 'A', 'G', 'E'},
            "[{\"a\":1},{\"a\":".getBytes(),
            "[{\"a\":1},,{\"a\":2}]".getBytes(),
            "[{\"a\":1}}]".getBytes()};
        for (byte[] b : garbage) {
            assertThrows(IOException.class, () -> records(b), new String(b, StandardCharsets.ISO_8859_1));
        }
    }

    @Test
    void overlongLineIsADecodeError() throws Exception {
        byte[] body = ("{\"a\":\"ok\"}\n" + "x".repeat(ObjectDecoder.MAX_LINE_BYTES + 1)).getBytes(StandardCharsets.UTF_8);
        try (ObjectDecoder.Records r = ObjectDecoder.open(body, true).records()) {
            assertEquals("{\"a\":\"ok\"}", r.next());
            assertThrows(IOException.class, r::next);
        }
        // a JSON array on one line is fine as long as each element fits
        String oneLine = "[" + ("{\"a\":\"" + "y".repeat(1000) + "\"},").repeat(20_000) + "{}]";
        assertTrue(oneLine.length() > ObjectDecoder.MAX_LINE_BYTES);
        assertEquals(20_001, records(oneLine.getBytes()).size());
    }

    @Test
    void checksums() {
        BytesArray data = new BytesArray("123456789");
        assertEquals("cbf43926", HexFormat.of().formatHex(Checksums.compute("crc32", data)));
        assertEquals("e3069283", HexFormat.of().formatHex(Checksums.compute("crc32c", data)));
        assertEquals("ae8b14860a799888", HexFormat.of().formatHex(Checksums.compute("crc64nvme", data)));
        assertNull(Checksums.compute("md4", data));
        byte[] md5 = SigV4.digest("MD5", data);
        assertNull(Checksums.verify(Map.of("x-amz-checksum-crc32", "y/Q5Jg=="), Map.of("x-amz-checksum-crc32c", "4waSgw=="), data, md5));
        assertEquals("BadDigest", Checksums.verify(Map.of(), Map.of("x-amz-checksum-crc32", "AAAAAA=="), data, md5).code());
        assertEquals("BadDigest", Checksums.verify(Map.of("content-md5", "AAAAAAAAAAAAAAAAAAAAAA=="), Map.of(), data, md5).code());
        assertNull(Checksums.verify(Map.of("x-amz-checksum-algorithm", "CRC32", "x-amz-checksum-type", "FULL_OBJECT"), Map.of(), data, md5));
    }

    /** The worked example in https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html: 64 KiB + 1 KiB of 'a' in chunks. */
    @Test
    void awsChunkedSignedExample() {
        String seed = "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9";
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.writeBytes(("10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n").getBytes());
        body.writeBytes("a".repeat(65536).getBytes());
        body.writeBytes(("\r\n400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n").getBytes());
        body.writeBytes("a".repeat(1024).getBytes());
        body.writeBytes(("\r\n0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n").getBytes());
        Map<String, String> headers = new java.util.HashMap<>(Map.of(
            "host", "s3.amazonaws.com",
            "x-amz-date", "20130524T000000Z",
            "x-amz-storage-class", "REDUCED_REDUNDANCY",
            "content-encoding", "aws-chunked",
            "x-amz-decoded-content-length", "66560",
            "content-length", "66824",
            "x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            "authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
                + "SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;"
                + "x-amz-storage-class,Signature=" + seed));
        AccessKeys keys = new AccessKeys();
        keys.set(List.of("AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));
        java.time.Instant at = java.time.Instant.parse("2013-05-24T00:00:00Z");
        SigV4.Result r = SigV4.verify("PUT", "/examplebucket/chunkObject.txt", "", headers, new BytesArray(body.toByteArray()), keys, at);
        assertNull(r.error(), String.valueOf(r.error()));
        assertEquals(66560, r.payload().length());
        assertEquals("a".repeat(66560), r.payload().utf8ToString());

        // one byte changed in a chunk breaks its signature
        byte[] tampered = body.toByteArray();
        tampered[200] = 'b';
        assertEquals("SignatureDoesNotMatch",
            SigV4.verify("PUT", "/examplebucket/chunkObject.txt", "", headers, new BytesArray(tampered), keys, at).error().code());
    }

    @Test
    void awsChunkedUnsignedWithTrailer() {
        String body = "5\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n";
        AwsChunked.Decoded d = AwsChunked.decode(new BytesArray(body), AwsChunked.UNSIGNED_TRAILER, Map.of("x-amz-decoded-content-length", "11"),
            new SigV4.Signer(new byte[32], "", "", ""));
        assertNull(d.error());
        assertEquals("hello world", d.payload().utf8ToString());
        assertEquals(Map.of("x-amz-checksum-crc32", "DUoRhQ=="), d.trailers());
        assertNull(Checksums.verify(Map.of(), d.trailers(), d.payload(), SigV4.digest("MD5", d.payload())));
        assertEquals("InvalidRequest", AwsChunked.decode(new BytesArray("5\r\nhel"), AwsChunked.UNSIGNED_TRAILER, Map.of(),
            new SigV4.Signer(new byte[32], "", "", "")).error().code());
    }

    @Test
    void normalizedUriIsAlsoTried() {
        List<String[]> c = SigV4.uriCandidates("/bk/fb/service=x/a b%2Fc.ndjson", "uploads");
        assertEquals(2, c.size());
        assertEquals("/bk/fb/service%3Dx/a%20b%2Fc.ndjson", c.get(1)[0]);
        assertEquals("uploads=", c.get(1)[1]);
        assertEquals(1, SigV4.uriCandidates("/bk/fb/service%3Dx/f.ndjson", "partNumber=1&uploadId=abc").size());
        assertEquals("%E2%82%AC~", SigV4.uriEncode("€~"));
    }

    @Test
    void s3Operations() {
        assertEquals(S3RestHandler.Op.PUT_OBJECT, S3RestHandler.op(RestRequest.Method.PUT, Map.of()));
        assertEquals(S3RestHandler.Op.PUT_OBJECT, S3RestHandler.op(RestRequest.Method.PUT, Map.of("x-id", "PutObject")));
        assertEquals(S3RestHandler.Op.UPLOAD_PART, S3RestHandler.op(RestRequest.Method.PUT,
            Map.of("partNumber", "1", "uploadId", "u", "x-id", "UploadPart")));
        assertEquals(S3RestHandler.Op.UPLOAD_PART, S3RestHandler.op(RestRequest.Method.PUT, Map.of("partNumber", "1", "uploadId", "u")));
        assertEquals(S3RestHandler.Op.CREATE_UPLOAD, S3RestHandler.op(RestRequest.Method.POST, Map.of("uploads", "")));
        assertEquals(S3RestHandler.Op.COMPLETE_UPLOAD, S3RestHandler.op(RestRequest.Method.POST, Map.of("uploadId", "u")));
        assertEquals(S3RestHandler.Op.ABORT_UPLOAD, S3RestHandler.op(RestRequest.Method.DELETE, Map.of("uploadId", "u")));
        assertNull(S3RestHandler.op(RestRequest.Method.PUT, Map.of("tagging", "")));
        assertNull(S3RestHandler.op(RestRequest.Method.DELETE, Map.of()));
        assertNull(S3RestHandler.op(RestRequest.Method.GET, Map.of()));
        assertEquals(Map.of("uploads", "", "x", "a b"), S3RestHandler.parseQuery("uploads&x=a%20b"));
    }

    @Test
    void completionXml() {
        String xml = "<?xml version=\"1.0\"?><CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
            + "<Part><ETag>\"0123456789ABCDEF0123456789abcdef\"</ETag><PartNumber>1</PartNumber></Part>"
            + "<Part><PartNumber>2</PartNumber><ChecksumCRC32>AAAAAA==</ChecksumCRC32><ETag>&quot;fedcba9876543210fedcba9876543210&quot;</ETag></Part>"
            + "<Part><ETag>&#34;00000000000000000000000000000003&#34;</ETag><PartNumber>3</PartNumber></Part>" // Go SDK
            + "<Part><ETag>&#x22;00000000000000000000000000000004&#x22;</ETag><PartNumber>4</PartNumber></Part>"
            + "</CompleteMultipartUpload>";
        assertEquals(List.of(new Multipart.Part(1, "0123456789abcdef0123456789abcdef"), new Multipart.Part(2, "fedcba9876543210fedcba9876543210"),
            new Multipart.Part(3, "00000000000000000000000000000003"), new Multipart.Part(4, "00000000000000000000000000000004")),
            Multipart.parseCompletion(xml));
        assertEquals(32, Multipart.newUploadId().length());
    }

    @Test
    void clusterFailuresAreRetriedLineFailuresAreNot() {
        // the target deleted mid-bulk (404) must not lose the lines: the retry recreates the data stream
        for (RestStatus st : List.of(RestStatus.NOT_FOUND, RestStatus.TOO_MANY_REQUESTS, RestStatus.SERVICE_UNAVAILABLE,
            RestStatus.UNAUTHORIZED, RestStatus.FORBIDDEN)) {
            assertEquals(true, S3Indexer.retryable(st), st.toString());
        }
        for (RestStatus st : List.of(RestStatus.BAD_REQUEST, RestStatus.CONFLICT, RestStatus.INTERNAL_SERVER_ERROR)) {
            assertEquals(false, S3Indexer.retryable(st), st.toString());
        }
    }

    @Test
    void onlyAnApiKeyCountsAsTheKey() {
        // no authentication (security off, or the internal user's context) must never pass for the key
        assertEquals(false, KeyCheckAction.isApiKey(null));
        assertEquals(false, KeyCheckAction.isApiKey("not an Authentication"));
    }
}
