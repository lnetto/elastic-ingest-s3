package co.elastic.s3http;

import io.airlift.compress.zstd.ZstdInputStream;
import org.elasticsearch.common.bytes.BytesArray;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.xxhash.XXHashFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/**
 * Opens an uploaded object as records: decompresses by magic bytes (gzip, zstd, lz4 or none), whatever the
 * key's extension says, then reads one record per line (ndjson, or raw text) or, when the object is a JSON
 * array, one per element.
 */
final class ObjectDecoder {

    private ObjectDecoder() {}

    /** The next record's text, or null at the end; {@link #number()} is its line (or array element) number, from 1. */
    interface Records extends java.io.Closeable {
        String next() throws IOException;

        int number();
    }

    /** The object's records, the compression they were read through, and how they were split: "lines" or "json_array". */
    record Opened(Records records, String compression, String format) {}

    static String sniffCompression(byte[] body) {
        return sniff(body, body.length);
    }

    private static String sniff(byte[] head, int n) {
        if (starts(head, n, 0x1f, 0x8b)) {
            return "gzip";
        }
        if (starts(head, n, 0x28, 0xb5, 0x2f, 0xfd)) {
            return "zstd";
        }
        if (starts(head, n, 0x04, 0x22, 0x4d, 0x18)) {
            return "lz4";
        }
        return "none"; // including an empty object, whatever its extension says
    }

    private static boolean starts(byte[] head, int n, int... magic) {
        if (n < magic.length) {
            return false;
        }
        for (int i = 0; i < magic.length; i++) {
            if ((head[i] & 0xff) != magic[i]) {
                return false;
            }
        }
        return true;
    }

    static InputStream decompress(InputStream in, String compression) throws IOException {
        return switch (compression) {
            case "gzip" -> new GZIPInputStream(in, 65536);
            case "zstd" -> new CorruptAsIOException(new ZstdInputStream(in));
            // pure-Java decoders: the JNI ones would need native-library entitlements
            case "lz4" -> new CorruptAsIOException(
                new LZ4FrameInputStream(in, LZ4Factory.safeInstance().safeDecompressor(), XXHashFactory.safeInstance().hash32()));
            default -> in;
        };
    }

    /** Longest line accepted, decompressed; far above any sane log line. */
    static final int MAX_LINE_BYTES = 16 << 20;
    /** Most records one object may hold: past it the object is a decode error, not hours of indexing. */
    static final int MAX_RECORDS = 50_000_000;
    /** Largest decompressed object: 1 GiB, or 100 times what was uploaded (an uncompressed object is never cut). */
    static final long MIN_DECOMPRESSED_CAP = 1L << 30;
    static final int MAX_RATIO = 100;

    /**
     * A multipart upload's part could not be read back (a node restarting, or a part another Complete
     * already deleted). Unlike corrupt data it is not the object's fault: it fails the request (5xx), so the
     * client retries and the parts are kept.
     */
    static final class PartUnavailable extends RuntimeException {
        PartUnavailable(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Fails the read once a line passes {@code max} bytes, or the object {@code maxTotal}. A small object can
     * decompress to gigabytes without a newline, and readLine() would hold it all: an OutOfMemoryError is fatal
     * to the node. Or with newlines, to hundreds of millions of lines, indexed long after the client gave up.
     */
    static final class LineLimit extends FilterInputStream {
        private final int max;
        private final long maxTotal;
        private long sinceNewline;
        private long total;
        /** Off for a JSON array, which may well be one line: its elements are limited instead. */
        volatile boolean enabled = true;

        LineLimit(InputStream in, int max, long maxTotal) {
            super(in);
            this.max = max;
            this.maxTotal = maxTotal;
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b >= 0) {
                see((byte) b);
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = in.read(b, off, len);
            for (int k = off; k < off + n; k++) {
                see(b[k]);
            }
            return n;
        }

        private void see(byte b) throws IOException {
            if (++total > maxTotal) {
                throw new IOException("object decompresses to more than " + maxTotal + " bytes");
            }
            if (enabled == false) {
                return;
            }
            sinceNewline = b == '\n' ? 0 : sinceNewline + 1;
            if (sinceNewline > max) {
                throw new IOException("line longer than " + max + " bytes");
            }
        }
    }

    /**
     * lz4-java and aircompressor report corrupt input as RuntimeException; as an IOException, like gzip's,
     * it reaches the indexer's decode-error handling instead of failing the request (which the client would retry forever).
     */
    private static final class CorruptAsIOException extends FilterInputStream {
        CorruptAsIOException(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            try {
                return in.read();
            } catch (PartUnavailable e) {
                throw e;
            } catch (RuntimeException e) {
                throw new IOException("corrupt " + e.getMessage(), e);
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            try {
                return in.read(b, off, len);
            } catch (PartUnavailable e) {
                throw e;
            } catch (RuntimeException e) {
                throw new IOException("corrupt " + e.getMessage(), e);
            }
        }
    }

    static Opened open(byte[] body, boolean jsonFile) throws IOException {
        return open(new BytesArray(body).streamInput(), body.length, jsonFile);
    }

    /**
     * @param raw      the object as uploaded, read once, front to back: the request's own buffer (never a copy, which
     *                 would double what each upload holds on the heap) or a multipart upload's parts, one at a time
     * @param size     its size as uploaded, for the decompressed-size cap
     * @param jsonFile the key names a .json file, as senders that write one JSON array per file name it: only then
     *                 is an object starting with '[' a JSON array; otherwise "[INFO] ..." is text
     */
    static Opened open(InputStream raw, long size, boolean jsonFile) throws IOException {
        BufferedInputStream in = new BufferedInputStream(raw, 65536);
        in.mark(4);
        byte[] head = new byte[4];
        int n = in.readNBytes(head, 0, 4);
        in.reset();
        String compression = sniff(head, n);
        LineLimit limit = new LineLimit(decompress(in, compression), MAX_LINE_BYTES,
            Math.max(MIN_DECOMPRESSED_CAP, Math.min(Long.MAX_VALUE / MAX_RATIO, size) * MAX_RATIO));
        BufferedReader r = new BufferedReader(new InputStreamReader(limit, StandardCharsets.UTF_8), 65536);
        // a .json file that starts with '[' is split into its elements, anything else into lines
        r.mark(8);
        int c = r.read();
        while (c == ' ' || c == '\n' || c == '\r' || c == '\t' || c == '\uFEFF') {
            r.mark(8);
            c = r.read();
        }
        if (c == '[' && jsonFile) {
            limit.enabled = false;
            return new Opened(new ArrayElements(r), compression, "json_array");
        }
        if (c != -1) {
            r.reset();
        }
        return new Opened(new Lines(r), compression, "lines");
    }

    /** One record per non-blank line. */
    private static final class Lines implements Records {
        private final BufferedReader in;
        private int number;

        Lines(BufferedReader in) {
            this.in = in;
        }

        @Override
        public String next() throws IOException {
            String line;
            while ((line = in.readLine()) != null) {
                if (++number > MAX_RECORDS) {
                    throw new IOException("more than " + MAX_RECORDS + " lines");
                }
                if (line.isBlank() == false) {
                    return line;
                }
            }
            return null;
        }

        @Override
        public int number() {
            return number;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    /**
     * The elements of a top-level JSON array (the opening '[' already read), each exactly as written: split by
     * tracking strings and nesting, not parsed, so nothing is reformatted and repeated keys (which some senders
     * write for multivalue fields) survive. A truncated array or one longer than MAX_LINE_BYTES is an IOException.
     */
    static final class ArrayElements implements Records {
        private final BufferedReader in;
        private int number;
        private boolean done;

        ArrayElements(BufferedReader in) {
            this.in = in;
        }

        @Override
        public String next() throws IOException {
            if (done) {
                return null;
            }
            StringBuilder out = new StringBuilder();
            int depth = 0;
            boolean inString = false;
            boolean escaped = false;
            int c;
            while ((c = in.read()) != -1) {
                if (inString) {
                    out.append((char) c);
                    if (escaped) {
                        escaped = false;
                    } else if (c == '\\') {
                        escaped = true;
                    } else if (c == '"') {
                        inString = false;
                    }
                } else if (depth == 0 && (c == ',' || c == ']')) {
                    String element = out.toString().strip();
                    if (c == ']') {
                        done = true;
                        // one array per object: anything after it would be lost behind a 200
                        for (int t; (t = in.read()) != -1; ) {
                            if (Character.isWhitespace(t) == false) {
                                throw new IOException("data after the JSON array, after element " + number);
                            }
                        }
                        if (element.isEmpty()) {
                            return null; // [] or a trailing comma
                        }
                    } else if (element.isEmpty()) {
                        throw new IOException("empty array element after element " + number);
                    }
                    if (++number > MAX_RECORDS) {
                        throw new IOException("more than " + MAX_RECORDS + " array elements");
                    }
                    return element;
                } else {
                    if (c == '"') {
                        inString = true;
                    } else if (c == '{' || c == '[') {
                        depth++;
                    } else if (c == '}' || c == ']') {
                        depth--;
                    }
                    if (depth < 0) {
                        throw new IOException("unbalanced JSON after element " + number);
                    }
                    out.append((char) c);
                }
                if (out.length() > MAX_LINE_BYTES) {
                    throw new IOException("array element longer than " + MAX_LINE_BYTES + " characters");
                }
            }
            throw new IOException("JSON array ends early, after element " + number);
        }

        @Override
        public int number() {
            return number;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
