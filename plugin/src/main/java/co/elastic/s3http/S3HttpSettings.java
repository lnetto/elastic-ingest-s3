package co.elastic.s3http;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.TimeValue;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.regex.Pattern;

/** The plugin's settings: where lines go, which access keys and buckets are accepted, and the key it writes with. */
public final class S3HttpSettings {

    private S3HttpSettings() {}

    /** The settings prefix, the field namespace of every document, and event.module. */
    static final String NS = "s3";
    /** The plugin's name. */
    static final String PLUGIN = "ingest-s3";

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{[A-Za-z0-9_.]{1,64}}");

    /**
     * Target index / data stream. {bucket} is replaced with the bucket, and {name} with the value of a
     * name=value segment of the object key (Hive-style partitions, e.g. .../service=cart/...): "unknown" when
     * the key has none. The plugin's index template matches logs-s3.*-*: keep that shape (e.g.
     * logs-s3.{bucket}-default) so the streams get its mappings, default pipeline and logs-s3@custom.
     */
    public static final Setting<String> TARGET = Setting.simpleString(
        NS + ".target",
        "logs-s3.generic-default",
        S3HttpSettings::validateTarget,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Refuses names Elasticsearch would reject per document: those failures are not retryable, so a bad
     * target would drop every line while the client is told it was stored.
     */
    static void validateTarget(String target) {
        String name = PLACEHOLDER.matcher(target).replaceAll("x");
        String problem = name.isEmpty() ? "is empty"
            : name.equals(name.toLowerCase(Locale.ROOT)) == false ? "must be lowercase"
            : name.chars().anyMatch(ch -> "\\/*?\"<>| ,#:{}".indexOf(ch) >= 0) ? "contains a character index names cannot have"
            : "-_+".indexOf(name.charAt(0)) >= 0 ? "must not start with - _ or +"
            : name.equals(".") || name.equals("..") ? "must not be . or .."
            : null;
        if (problem != null) {
            throw new IllegalArgumentException(NS + ".target [" + target + "] " + problem + " (placeholders: {bucket}, {<key partition>})");
        }
    }

    /**
     * S3 credentials clients sign with, "ACCESS_KEY_ID:SECRET" or "name=ACCESS_KEY_ID:SECRET". SigV4 needs
     * the secret itself, so the setting is filtered: GET _cluster/settings never returns it.
     */
    public static final Setting<List<String>> ACCESS_KEYS = Setting.listSetting(
        NS + ".access_keys",
        List.of(),
        Function.identity(),
        Property.NodeScope,
        Property.Dynamic,
        Property.Filtered
    );

    /** Bucket names accepted (the first path segment); empty accepts any. */
    public static final Setting<List<String>> BUCKETS = Setting.stringListSetting(
        NS + ".buckets",
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * How long a retryable failure (503 SlowDown, 500) or a wrong signature for a known access key is held
     * before it is answered. Some senders re-upload at once, with no backoff of their own, so without this a
     * busy cluster gets hammered. Other refusals (no signature, an unknown key) are answered at once: a held
     * answer keeps the request's body in memory.
     */
    public static final Setting<TimeValue> RETRY_DELAY = Setting.timeSetting(
        NS + ".retry_delay",
        TimeValue.timeValueSeconds(10),
        TimeValue.ZERO,
        Property.NodeScope,
        Property.Dynamic
    );

    /** Multipart uploads neither completed nor aborted are deleted after this long, with their parts. */
    public static final Setting<TimeValue> MULTIPART_EXPIRY = Setting.timeSetting(
        NS + ".multipart_expiry",
        TimeValue.timeValueHours(24),
        TimeValue.timeValueMinutes(1),
        Property.NodeScope,
        Property.Dynamic
    );

    /** Encoded Elasticsearch API key the plugin writes with (keystore). */
    public static final Setting<SecureString> API_KEY = SecureSetting.secureString(NS + ".api_key", null);

    /**
     * What the plugin adds to each line. "full": where it came from (bucket, key, ETag, line, format,
     * compression, partitions) and the request (id, time, node, address, access key, user agent, size), plus
     * event.ingested. "minimal": bucket, key, line and the access key's name only; indexing is faster, since
     * those fields cost about as much as the line itself.
     */
    public static final Setting<String> METADATA = new Setting<>(
        NS + ".metadata",
        "full",
        v -> {
            if (v.equals("full") == false && v.equals("minimal") == false) {
                throw new IllegalArgumentException(NS + ".metadata must be [full] or [minimal], not [" + v + "]");
            }
            return v;
        },
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Whether the elected master installs (and upgrades) the plugin's index template, logs-s3.*-*, and its default pipeline. Turn it
     * off to manage them yourself.
     */
    public static final Setting<Boolean> TEMPLATES_ENABLED = Setting.boolSetting(
        NS + ".templates.enabled",
        true,
        Property.NodeScope,
        Property.Dynamic
    );

    public static List<Setting<?>> all() {
        return List.of(TARGET, METADATA, TEMPLATES_ENABLED, ACCESS_KEYS, BUCKETS, RETRY_DELAY, MULTIPART_EXPIRY, API_KEY);
    }
}
