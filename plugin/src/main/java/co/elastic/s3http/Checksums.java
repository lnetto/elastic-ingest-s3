package co.elastic.s3http;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

/**
 * The integrity checks a client may send with an upload: Content-MD5, and an x-amz-checksum-* header or
 * aws-chunked trailer (CRC32, CRC32C, CRC64NVME, SHA1, SHA256), all base64. A mismatch means the bytes
 * changed on the way: the upload is refused (400 BadDigest) and the client sends it again.
 */
final class Checksums {

    private Checksums() {}

    /** @param md5 the payload's MD5, already computed for the ETag */
    static S3Error verify(Map<String, String> headers, Map<String, String> trailers, BytesReference payload, byte[] md5) {
        String contentMd5 = headers.get("content-md5");
        if (contentMd5 != null && Base64.getEncoder().encodeToString(md5).equals(contentMd5.trim()) == false) {
            return new S3Error(400, "BadDigest", "The Content-MD5 you specified did not match what we received.");
        }
        for (Map<String, String> source : java.util.List.of(headers, trailers)) {
            for (Map.Entry<String, String> e : source.entrySet()) {
                String name = e.getKey().toLowerCase(Locale.ROOT);
                if (name.startsWith("x-amz-checksum-") == false || name.equals("x-amz-checksum-type")
                    || name.equals("x-amz-checksum-algorithm")) {
                    continue;
                }
                byte[] actual = compute(name.substring("x-amz-checksum-".length()), payload);
                if (actual != null && Base64.getEncoder().encodeToString(actual).equals(e.getValue().trim()) == false) {
                    return new S3Error(400, "BadDigest", "The " + name.substring("x-amz-checksum-".length()).toUpperCase(Locale.ROOT)
                        + " you specified did not match the calculated checksum.");
                }
            }
        }
        return null;
    }

    /** The checksum as S3 encodes it before base64 (big-endian for the CRCs); null for an algorithm not known here. */
    static byte[] compute(String algorithm, BytesReference data) {
        return switch (algorithm) {
            case "crc32" -> crc(new CRC32(), data);
            case "crc32c" -> crc(new CRC32C(), data);
            case "crc64nvme" -> ByteBuffer.allocate(8).putLong(crc64nvme(data)).array();
            case "sha1" -> SigV4.digest("SHA-1", data);
            case "sha256" -> SigV4.digest("SHA-256", data);
            default -> null;
        };
    }

    private static byte[] crc(Checksum c, BytesReference data) {
        each(data, r -> c.update(r.bytes, r.offset, r.length));
        return ByteBuffer.allocate(4).putInt((int) c.getValue()).array();
    }

    // CRC-64/NVME: reflected polynomial 0x9a6c9329ac4bc9b5, initial value and final xor all ones
    private static final long[] CRC64 = new long[256];

    static {
        for (int i = 0; i < 256; i++) {
            long c = i;
            for (int k = 0; k < 8; k++) {
                c = (c & 1) != 0 ? (c >>> 1) ^ 0x9a6c9329ac4bc9b5L : c >>> 1;
            }
            CRC64[i] = c;
        }
    }

    static long crc64nvme(BytesReference data) {
        long[] crc = {~0L};
        each(data, r -> {
            long c = crc[0];
            for (int i = r.offset; i < r.offset + r.length; i++) {
                c = CRC64[(int) ((c ^ r.bytes[i]) & 0xff)] ^ (c >>> 8);
            }
            crc[0] = c;
        });
        return ~crc[0];
    }

    private static void each(BytesReference data, java.util.function.Consumer<BytesRef> f) {
        try {
            BytesRefIterator it = data.iterator();
            for (BytesRef r = it.next(); r != null; r = it.next()) {
                f.accept(r);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
