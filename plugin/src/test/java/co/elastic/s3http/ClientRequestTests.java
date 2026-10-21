package co.elastic.s3http;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Requests as real clients send them: the AWS CLI 2.37 over HTTP (signed payload hash, CRC64NVME header) and over HTTPS
 * (aws-chunked body with a CRC64NVME trailer, as AWS SDKs send by default) and its multipart requests, and
 * Fluent Bit 3.2, which sends '=' in the path unencoded but signs it encoded.
 */
class ClientRequestTests {

    static final String SECRET = "testsecret0123456789";

    record ClientRequest(String method, String rawPath, String rawQuery, Map<String, String> headers, byte[] body) {
        Instant signedAt() {
            return Instant.from(DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC).parse(headers.get("x-amz-date")));
        }

        SigV4.Result verify(String secret, Instant now) {
            AccessKeys keys = new AccessKeys();
            keys.set(List.of("cli=AKIDTEST01:" + secret));
            return SigV4.verify(method, rawPath, rawQuery, headers, new BytesArray(body), keys, now);
        }

        SigV4.Result verify() {
            return verify(SECRET, signedAt());
        }
    }

    static ClientRequest request(String name) throws IOException {
        Path dir = Path.of("src/test/resources/requests");
        Map<String, Object> rec = XContentHelper.convertToMap(JsonXContent.jsonXContent, Files.readString(dir.resolve(name + ".json")), false);
        Map<String, String> headers = new HashMap<>();
        @SuppressWarnings("unchecked")
        Map<String, Object> h = (Map<String, Object>) rec.get("headers");
        h.forEach((k, v) -> headers.put(k.toLowerCase(Locale.ROOT), (String) v));
        return new ClientRequest((String) rec.get("method"), (String) rec.get("path"), (String) rec.get("query"), headers,
            Files.readAllBytes(dir.resolve(name + ".bin")));
    }

    static final List<String> ALL = List.of("awscli_put_signed", "awscli_put_chunked_trailer", "awscli_create", "awscli_complete",
        "fluentbit_create");

    @Test
    void everyRequestVerifies() throws IOException {
        for (String name : ALL) {
            SigV4.Result r = request(name).verify();
            assertNull(r.error(), name + ": " + r.error());
            assertEquals("cli", r.key().name(), name);
        }
    }

    @Test
    void chunkedUploadDecodesToTheFileWithItsChecksum() throws IOException {
        ClientRequest chunked = request("awscli_put_chunked_trailer");
        ClientRequest plain = request("awscli_put_signed");
        SigV4.Result r = chunked.verify();
        assertEquals(new BytesArray(plain.body()), r.payload()); // the same file, uploaded both ways
        assertEquals(Map.of("x-amz-checksum-crc64nvme", "v0MKQ6JlMhU="), r.trailers());
        assertNull(Checksums.verify(chunked.headers(), r.trailers(), r.payload(), SigV4.digest("MD5", r.payload())));
        // over HTTP the CLI sends the same checksum as a header
        assertNull(Checksums.verify(plain.headers(), Map.of(), new BytesArray(plain.body()), SigV4.digest("MD5", new BytesArray(plain.body()))));

        byte[] tampered = chunked.body().clone();
        tampered[100] ^= 1; // unsigned chunks: the trailer checksum is what catches it
        SigV4.Result t = new ClientRequest(chunked.method(), chunked.rawPath(), chunked.rawQuery(), chunked.headers(), tampered).verify();
        assertNull(t.error());
        assertEquals("BadDigest", Checksums.verify(chunked.headers(), t.trailers(), t.payload(), SigV4.digest("MD5", t.payload())).code());
    }

    @Test
    void completionListsTheParts() throws IOException {
        List<Multipart.Part> parts = Multipart.parseCompletion(new String(request("awscli_complete").body(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(4, parts.size());
        assertEquals(List.of(1, 2, 3, 4), parts.stream().map(Multipart.Part::number).toList());
    }

    @Test
    void badRequestsAreRejected() throws IOException {
        ClientRequest c = request("awscli_put_signed");
        assertEquals("SignatureDoesNotMatch", c.verify("wrong", c.signedAt()).error().code());
        assertEquals("SignatureDoesNotMatch",
            new ClientRequest(c.method(), c.rawPath() + "x", "", c.headers(), c.body()).verify().error().code());
        byte[] tampered = c.body().clone();
        tampered[tampered.length - 1] ^= 1;
        assertEquals("XAmzContentSHA256Mismatch", new ClientRequest(c.method(), c.rawPath(), "", c.headers(), tampered).verify().error().code());
        assertEquals("RequestTimeTooSkewed", c.verify(SECRET, c.signedAt().plusSeconds(16 * 60)).error().code());
        Map<String, String> noAuth = new HashMap<>(c.headers());
        noAuth.remove("authorization");
        assertEquals("AccessDenied", new ClientRequest(c.method(), c.rawPath(), "", noAuth, c.body()).verify().error().code());
        AccessKeys other = new AccessKeys();
        other.set(List.of("someoneelse:" + SECRET));
        assertEquals("InvalidAccessKeyId",
            SigV4.verify(c.method(), c.rawPath(), "", c.headers(), new BytesArray(c.body()), other, c.signedAt()).error().code());
    }

    /** Elastic Cloud's proxy replaces Host with the node's address and passes the original on. */
    @Test
    void signaturesSurviveAHostRewritingProxy() throws IOException {
        for (String name : ALL) {
            ClientRequest c = request(name);
            Map<String, String> h = new HashMap<>(c.headers());
            String signedHost = h.put("host", "172.18.141.83:18490");
            assertEquals("SignatureDoesNotMatch", new ClientRequest(c.method(), c.rawPath(), c.rawQuery(), h, c.body()).verify().error().code(), name);
            h.put("x-forwarded-host", signedHost);
            h.put("x-forwarded-proto", "https");
            assertNull(new ClientRequest(c.method(), c.rawPath(), c.rawQuery(), h, c.body()).verify().error(), name);
        }
    }

    @Test
    void hostCandidatesCoverTheDefaultPort() {
        assertEquals(List.of("10.0.0.1:1", "es.example.com", "es.example.com:443"),
            SigV4.hostCandidates(Map.of("host", "10.0.0.1:1", "x-forwarded-host", "es.example.com", "x-forwarded-proto", "https")));
        assertEquals(List.of("10.0.0.1:1", "es.example.com:443", "es.example.com"),
            SigV4.hostCandidates(Map.of("host", "10.0.0.1:1", "x-forwarded-host", "es.example.com:443")));
        assertEquals(List.of("es.example.com"), SigV4.hostCandidates(Map.of("host", "es.example.com")));
    }
}
