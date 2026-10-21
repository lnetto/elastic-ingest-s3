package co.elastic.s3http;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * AWS Signature Version 4 verification for S3 requests signed in the Authorization header
 * (https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html). S3 does not
 * double-encode the path, so the canonical URI is the path exactly as it arrived on the wire.
 */
final class SigV4 {

    /**
     * The key that signed, and the object's bytes ({@code payload}: the body itself, or its data once the
     * aws-chunked framing is taken off) with any trailing headers; or an S3 error.
     */
    record Result(AccessKeys.Key key, S3Error error, BytesReference payload, Map<String, String> trailers) {
        static Result fail(String code, int status, String message) {
            return new Result(null, new S3Error(status, code, message), null, Map.of());
        }

        static Result fail(S3Error error) {
            return new Result(null, error, null, Map.of());
        }
    }

    /** What an aws-chunked body's chunk and trailer signatures chain from. */
    record Signer(byte[] signingKey, String amzDate, String scope, String seedSignature) {
        String sign(String stringToSign) {
            return HEX.formatHex(hmac(signingKey, stringToSign));
        }
    }

    static final Duration MAX_SKEW = Duration.ofMinutes(15);
    static final String UNSIGNED = "UNSIGNED-PAYLOAD";
    private static final DateTimeFormatter AMZ_DATE = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    private static final HexFormat HEX = HexFormat.of();

    private SigV4() {}

    /**
     * @param headers request headers, names lowercased
     * @param rawPath path as sent, still percent-encoded
     * @param rawQuery query string as sent, without '?', or ""
     */
    /** @param body the request's own buffer, read in place: copying it would double what each upload holds on the heap */
    static Result verify(String method, String rawPath, String rawQuery, Map<String, String> headers, BytesReference body, AccessKeys keys,
        Instant now) {
        String auth = headers.get("authorization");
        if (auth == null || auth.isBlank()) {
            return Result.fail("AccessDenied", 403, "Requests must be signed with AWS Signature Version 4");
        }
        if (auth.startsWith("AWS4-HMAC-SHA256 ") == false) {
            return Result.fail("InvalidRequest", 400, "Only AWS4-HMAC-SHA256 header signatures are supported");
        }
        String credential = param(auth, "Credential");
        String signedHeaders = param(auth, "SignedHeaders");
        String signature = param(auth, "Signature");
        if (credential == null || signedHeaders == null || signature == null) {
            return Result.fail("AuthorizationHeaderMalformed", 400, "The authorization header is malformed");
        }
        String[] scope = credential.split("/");
        if (scope.length != 5 || "aws4_request".equals(scope[4]) == false) {
            return Result.fail("AuthorizationHeaderMalformed", 400, "The authorization header credential scope is malformed");
        }
        AccessKeys.Key key = keys.get(scope[0]);
        if (key == null) {
            return Result.fail("InvalidAccessKeyId", 403, "The AWS Access Key Id you provided does not exist in our records.");
        }
        String amzDate = headers.get("x-amz-date");
        if (amzDate == null) {
            return Result.fail("AccessDenied", 403, "x-amz-date header is required");
        }
        Instant signedAt;
        try {
            signedAt = Instant.from(AMZ_DATE.parse(amzDate));
        } catch (DateTimeParseException e) {
            return Result.fail("AccessDenied", 403, "x-amz-date is not a valid ISO 8601 basic timestamp");
        }
        if (Duration.between(signedAt, now).abs().compareTo(MAX_SKEW) > 0) {
            return Result.fail("RequestTimeTooSkewed", 403, "The difference between the request time and the server's time is too large.");
        }
        if (amzDate.startsWith(scope[1]) == false) {
            return Result.fail("AuthorizationHeaderMalformed", 400, "Credential date does not match x-amz-date");
        }
        String payloadHash = headers.get("x-amz-content-sha256");
        if (payloadHash == null) {
            return Result.fail("InvalidRequest", 400, "Missing required header for this request: x-amz-content-sha256");
        }
        boolean streaming = payloadHash.startsWith("STREAMING-");
        if (streaming && AwsChunked.MODES.contains(payloadHash) == false) {
            return Result.fail("NotImplemented", 501, "x-amz-content-sha256 [" + payloadHash + "] is not supported (SigV4a is not)");
        }
        if (streaming == false && UNSIGNED.equals(payloadHash) == false && MessageDigest.isEqual(
            HEX.formatHex(sha256(body)).getBytes(StandardCharsets.US_ASCII),
            payloadHash.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.US_ASCII)) == false) {
            return Result.fail("XAmzContentSHA256Mismatch", 400, "The provided 'x-amz-content-sha256' header does not match what was computed.");
        }

        for (String name : signedHeaders.split(";")) {
            if (headers.get(name) == null) {
                return Result.fail("AccessDenied", 403, "Signed header [" + name + "] is missing");
            }
        }
        byte[] k = hmac(("AWS4" + key.secret()).getBytes(StandardCharsets.UTF_8), scope[1]);
        k = hmac(k, scope[2]);
        k = hmac(k, scope[3]);
        k = hmac(k, "aws4_request");
        byte[] provided = signature.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.US_ASCII);
        String scopeString = scope[1] + "/" + scope[2] + "/" + scope[3] + "/aws4_request";
        for (String[] uri : uriCandidates(rawPath, rawQuery))
        for (String host : hostCandidates(headers)) {
            String canonical = canonicalRequest(method, uri[0], uri[1], headers, signedHeaders, payloadHash, host);
            String stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scopeString + "\n"
                + HEX.formatHex(sha256(canonical.getBytes(StandardCharsets.UTF_8)));
            if (MessageDigest.isEqual(HEX.formatHex(hmac(k, stringToSign)).getBytes(StandardCharsets.US_ASCII), provided)) {
                if (streaming == false) {
                    return new Result(key, null, body, Map.of());
                }
                AwsChunked.Decoded d = AwsChunked.decode(body, payloadHash, headers, new Signer(k, amzDate, scopeString, signature.toLowerCase(Locale.ROOT)));
                return d.error() != null ? Result.fail(d.error()) : new Result(key, null, d.payload(), d.trailers());
            }
        }
        return Result.fail("SignatureDoesNotMatch", 403,
            "The request signature we calculated does not match the signature you provided. Check your key and signing method.");
    }

    /**
     * The Host values the client may have signed. Behind a reverse proxy that rewrites Host (Elastic
     * Cloud sends the node's internal address), the original is in X-Forwarded-Host; clients differ on
     * whether a default port is part of it. Trying them does not weaken anything: a match still needs
     * the secret.
     */
    static List<String> hostCandidates(Map<String, String> headers) {
        List<String> out = new ArrayList<>();
        out.add(headers.get("host"));
        String fwd = headers.get("x-forwarded-host");
        if (fwd != null && fwd.isBlank() == false) {
            String h = fwd.split(",")[0].trim();
            out.add(h);
            String proto = headers.getOrDefault("x-forwarded-proto", "https");
            String port = "http".equalsIgnoreCase(proto) ? ":80" : ":443";
            if (h.endsWith(port)) {
                out.add(h.substring(0, h.length() - port.length()));
            } else if (h.lastIndexOf(':') <= h.lastIndexOf(']')) {
                out.add(h + port);
            }
        }
        return out;
    }

    private static String canonicalRequest(String method, String rawPath, String rawQuery, Map<String, String> headers, String signedHeaders,
        String payloadHash, String host) {
        StringBuilder canonical = new StringBuilder();
        canonical.append(method).append('\n').append(rawPath.isEmpty() ? "/" : rawPath).append('\n').append(canonicalQuery(rawQuery)).append('\n');
        for (String name : signedHeaders.split(";")) {
            String v = "host".equals(name) && host != null ? host : headers.get(name);
            canonical.append(name).append(':').append(v.trim().replaceAll(" +", " ")).append('\n');
        }
        canonical.append('\n').append(signedHeaders).append('\n').append(payloadHash);
        return canonical.toString();
    }

    /**
     * The path and query the client may have signed: as sent (most clients, AWS SDKs), or normalized the way
     * the SigV4 spec says (each segment and parameter decoded and re-encoded, only A-Z a-z 0-9 - _ . ~ left
     * as they are), which clients that send characters such as '=' unencoded sign instead (Fluent Bit).
     */
    static List<String[]> uriCandidates(String rawPath, String rawQuery) {
        String[] raw = {rawPath, rawQuery};
        StringBuilder path = new StringBuilder();
        for (String segment : rawPath.split("/", -1)) {
            path.append(path.isEmpty() && segment.isEmpty() ? "" : "/").append(uriEncode(decode(segment)));
        }
        List<String> params = new ArrayList<>();
        if (rawQuery != null && rawQuery.isEmpty() == false) {
            for (String p : rawQuery.split("&")) {
                int eq = p.indexOf('=');
                params.add(uriEncode(decode(eq < 0 ? p : p.substring(0, eq))) + "=" + (eq < 0 ? "" : uriEncode(decode(p.substring(eq + 1)))));
            }
        }
        String[] normalized = {path.toString(), String.join("&", params)};
        return normalized[0].equals(rawPath) && canonicalQuery(normalized[1]).equals(canonicalQuery(rawQuery)) ? List.<String[]>of(raw)
            : List.of(raw, normalized);
    }

    private static String decode(String s) {
        try {
            return java.net.URLDecoder.decode(s.replace("+", "%2B"), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return s;
        }
    }

    static String uriEncode(String s) {
        StringBuilder out = new StringBuilder();
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xff;
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~') {
                out.append((char) c);
            } else {
                out.append('%').append(Character.toUpperCase(Character.forDigit(c >> 4, 16))).append(Character.toUpperCase(Character.forDigit(c & 15, 16)));
            }
        }
        return out.toString();
    }

    /** Query parameters sorted by name then value; values stay as the client encoded them. */
    static String canonicalQuery(String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            return "";
        }
        List<String> parts = new ArrayList<>(Arrays.asList(rawQuery.split("&")));
        parts.replaceAll(p -> p.indexOf('=') < 0 ? p + "=" : p);
        parts.sort(String::compareTo);
        return String.join("&", parts);
    }

    private static String param(String auth, String name) {
        for (String part : auth.substring(auth.indexOf(' ') + 1).split(",")) {
            String p = part.trim();
            if (p.startsWith(name + "=")) {
                return p.substring(name.length() + 1);
            }
        }
        return null;
    }

    static byte[] sha256(BytesReference data) {
        return digest("SHA-256", data);
    }

    /** A digest of a (possibly multi-buffer) BytesReference, without copying it into one array. */
    static byte[] digest(String algorithm, BytesReference data) {
        try {
            MessageDigest md = MessageDigest.getInstance(algorithm);
            BytesRefIterator it = data.iterator();
            for (BytesRef r = it.next(); r != null; r = it.next()) {
                md.update(r.bytes, r.offset, r.length);
            }
            return md.digest();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] hmac(byte[] key, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
