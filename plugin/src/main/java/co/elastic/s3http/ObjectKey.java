package co.elastic.s3http;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The bucket and key of an upload, and the Hive-style partitions in the key: every {@code name=value}
 * directory segment, e.g. {@code app/year=2026/month=10/service=cart/part-0001.ndjson.zst}.
 */
record ObjectKey(String bucket, String key, Map<String, String> partition) {

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_.]{1,64}");

    /** @param segments decoded path segments: bucket, then the key's segments */
    static ObjectKey parse(List<String> segments) {
        List<String> parts = segments.subList(1, segments.size());
        Map<String, String> partition = new LinkedHashMap<>();
        for (String d : parts.subList(0, parts.size() - 1)) {
            int eq = d.indexOf('=');
            if (eq > 0 && NAME.matcher(d.substring(0, eq)).matches()) {
                partition.put(d.substring(0, eq), d.substring(eq + 1));
            }
        }
        return new ObjectKey(segments.get(0), String.join("/", parts), partition);
    }

    /** The file is .json, compressed or not (x.json, x.json.zst): a JSON array per object, rather than lines. */
    boolean jsonFile() {
        String name = key.substring(key.lastIndexOf('/') + 1).toLowerCase(java.util.Locale.ROOT);
        for (String ext : new String[] {".gz", ".gzip", ".zst", ".zstd", ".lz4"}) {
            if (name.endsWith(ext)) {
                name = name.substring(0, name.length() - ext.length());
                break;
            }
        }
        return name.endsWith(".json");
    }
}
