package co.elastic.s3http;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** The allowed S3 credentials from {@code s3.access_keys}, by access key id. */
final class AccessKeys {

    record Key(String name, String id, String secret) {}

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_.:@-]{1,128}");
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9_.-]{3,128}");

    private volatile Map<String, Key> byId = Map.of();

    void set(List<String> entries) {
        byId = parse(entries);
    }

    Key get(String id) {
        return byId.get(id);
    }

    /** Called by the cluster settings validator: a bad entry fails the PUT instead of being ignored. */
    static void validate(List<String> entries) {
        parse(entries);
    }

    static Map<String, Key> parse(List<String> entries) {
        Map<String, Key> out = new HashMap<>();
        for (String raw : entries) {
            String e = raw.trim();
            String name = null;
            int eq = e.indexOf('=');
            if (eq >= 0) {
                name = e.substring(0, eq);
                e = e.substring(eq + 1);
                if (NAME.matcher(name).matches() == false) {
                    throw new IllegalArgumentException(S3HttpSettings.ACCESS_KEYS.getKey() + ": invalid name [" + name + "]; use letters, digits and _ . : @ -");
                }
            }
            int colon = e.indexOf(':');
            if (colon <= 0 || colon == e.length() - 1) {
                throw new IllegalArgumentException(S3HttpSettings.ACCESS_KEYS.getKey() + ": expected [name=]ACCESS_KEY_ID:SECRET");
            }
            String id = e.substring(0, colon);
            if (ID.matcher(id).matches() == false) {
                throw new IllegalArgumentException(S3HttpSettings.ACCESS_KEYS.getKey() + ": invalid access key id [" + id + "]");
            }
            if (out.put(id, new Key(name == null ? id : name, id, e.substring(colon + 1))) != null) {
                throw new IllegalArgumentException(S3HttpSettings.ACCESS_KEYS.getKey() + ": access key id [" + id + "] is listed twice");
            }
        }
        return Map.copyOf(out);
    }
}
