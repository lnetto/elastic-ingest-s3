package co.elastic.s3http;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The aws-chunked body encoding current AWS SDKs use for uploads
 * (https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html): the data in chunks, each
 * {@code <hex size>[;chunk-signature=<sig>]\r\n<data>\r\n}, a last chunk of size 0, then for the -TRAILER
 * modes trailing headers (a checksum, and in the signed mode its signature) and an empty line. Each chunk
 * signature chains from the one before, starting at the request's own signature, so no chunk can be
 * changed, dropped or reordered. The data is returned as slices of the request's buffer: never a copy.
 */
final class AwsChunked {

    static final String UNSIGNED_TRAILER = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
    static final String SIGNED = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
    static final String SIGNED_TRAILER = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
    static final Set<String> MODES = Set.of(UNSIGNED_TRAILER, SIGNED, SIGNED_TRAILER);

    private static final HexFormat HEX = HexFormat.of();
    private static final String EMPTY_SHA256 = HEX.formatHex(SigV4.sha256(new byte[0]));
    private static final int MAX_HEADER_LINE = 4096;

    record Decoded(BytesReference payload, Map<String, String> trailers, S3Error error) {
        static Decoded fail(String message) {
            return new Decoded(null, Map.of(), new S3Error(400, "InvalidRequest", "aws-chunked body: " + message));
        }

        static Decoded badSignature(String what) {
            return new Decoded(null, Map.of(), new S3Error(403, "SignatureDoesNotMatch", "The " + what + " signature does not match"));
        }
    }

    private AwsChunked() {}

    static Decoded decode(BytesReference body, String mode, Map<String, String> headers, SigV4.Signer signer) {
        boolean signed = mode.equals(UNSIGNED_TRAILER) == false;
        boolean trailer = mode.endsWith("-TRAILER");
        String previous = signer.seedSignature();
        List<BytesReference> chunks = new ArrayList<>();
        long total = 0;
        int pos = 0;
        while (true) {
            int eol = lineEnd(body, pos);
            if (eol < 0) {
                return Decoded.fail("chunk header without CRLF");
            }
            String header = body.slice(pos, eol - pos).utf8ToString();
            pos = eol + 2;
            int semi = header.indexOf(';');
            int size;
            try {
                size = Integer.parseInt(semi < 0 ? header.trim() : header.substring(0, semi).trim(), 16);
            } catch (NumberFormatException e) {
                return Decoded.fail("bad chunk size [" + header + "]");
            }
            if (size < 0 || size > body.length() - pos) {
                return Decoded.fail("chunk size [" + size + "] runs past the body");
            }
            BytesReference data = body.slice(pos, size);
            if (signed) {
                String given = semi < 0 ? null : param(header.substring(semi + 1), "chunk-signature");
                String expected = signer.sign("AWS4-HMAC-SHA256-PAYLOAD\n" + signer.amzDate() + "\n" + signer.scope() + "\n" + previous + "\n"
                    + EMPTY_SHA256 + "\n" + HEX.formatHex(SigV4.sha256(data)));
                if (given == null || equal(expected, given) == false) {
                    return Decoded.badSignature("chunk");
                }
                previous = expected;
            }
            pos += size;
            if (size == 0) {
                break;
            }
            if (pos + 2 > body.length() || body.get(pos) != '\r' || body.get(pos + 1) != '\n') {
                return Decoded.fail("chunk data not followed by CRLF");
            }
            pos += 2;
            chunks.add(data);
            total += size;
        }

        Map<String, String> trailers = new LinkedHashMap<>();
        String trailerSignature = null;
        StringBuilder canonical = new StringBuilder();
        while (true) {
            int eol = lineEnd(body, pos);
            if (eol < 0) {
                // some clients end the body right after the last trailer
                if (pos == body.length()) {
                    break;
                }
                return Decoded.fail("trailer without CRLF");
            }
            String line = body.slice(pos, eol - pos).utf8ToString();
            pos = eol + 2;
            if (line.isEmpty()) {
                break;
            }
            int colon = line.indexOf(':');
            if (colon <= 0) {
                return Decoded.fail("bad trailer [" + line + "]");
            }
            String name = line.substring(0, colon).trim().toLowerCase(Locale.ROOT);
            String value = line.substring(colon + 1).trim();
            if (name.equals("x-amz-trailer-signature")) {
                trailerSignature = value;
            } else {
                trailers.put(name, value);
                canonical.append(name).append(':').append(value).append('\n');
            }
        }
        if (trailer == false && trailers.isEmpty() == false) {
            return Decoded.fail("trailing headers in a mode without them");
        }
        if (trailer && signed) {
            String expected = signer.sign("AWS4-HMAC-SHA256-TRAILER\n" + signer.amzDate() + "\n" + signer.scope() + "\n" + previous + "\n"
                + HEX.formatHex(SigV4.sha256(canonical.toString().getBytes(StandardCharsets.UTF_8))));
            if (trailerSignature == null || equal(expected, trailerSignature) == false) {
                return Decoded.badSignature("trailer");
            }
        }
        String declared = headers.get("x-amz-decoded-content-length");
        if (declared != null && declared.trim().equals(Long.toString(total)) == false) {
            return Decoded.fail("x-amz-decoded-content-length [" + declared + "] but the chunks hold [" + total + "] bytes");
        }
        BytesReference payload = chunks.isEmpty() ? BytesArray.EMPTY
            : chunks.size() == 1 ? chunks.get(0)
            : CompositeBytesReference.of(chunks.toArray(new BytesReference[0]));
        return new Decoded(payload, trailers, null);
    }

    /** Index of the CR of the next CRLF at or after {@code from}, within a header-sized window; -1 if none. */
    private static int lineEnd(BytesReference body, int from) {
        int limit = Math.min(body.length() - 1, from + MAX_HEADER_LINE);
        for (int i = from; i < limit; i++) {
            if (body.get(i) == '\r' && body.get(i + 1) == '\n') {
                return i;
            }
        }
        return -1;
    }

    private static String param(String params, String name) {
        for (String p : params.split(";")) {
            int eq = p.indexOf('=');
            if (eq > 0 && p.substring(0, eq).trim().equals(name)) {
                return p.substring(eq + 1).trim();
            }
        }
        return null;
    }

    private static boolean equal(String expected, String given) {
        return MessageDigest.isEqual(expected.getBytes(StandardCharsets.US_ASCII), given.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.US_ASCII));
    }
}
