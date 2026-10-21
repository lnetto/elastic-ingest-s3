package co.elastic.s3http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Indexes one uploaded ndjson object: one document per line, bulked in chunks as the keystore API key.
 * Document ids are derived from the object (bucket, key and ETag) and line number, so when a client
 * re-uploads an object after an error, lines that were already indexed come back as version conflicts and
 * are skipped instead of duplicated; different content under the same key is new documents.
 */
final class S3Indexer {

    private static final Logger logger = LogManager.getLogger(S3Indexer.class);

    /** What the upload produced; {@code retryable} means answer 503 so the client resends the object. */
    record Result(int lines, int indexed, int duplicates, int dropped, int invalid, boolean retryable) {}

    /** The HTTP request the object came in (for a multipart upload, the one completing it), and the object's ETag and size. */
    record Request(String id, long receivedMillis, String node, String remoteAddress, AccessKeys.Key key, long bytes, String userAgent,
        String etag) {}

    static final int CHUNK_DOCS = 5000;
    static final long CHUNK_BYTES = 8L << 20;
    /** Bulks of one object sent before the first is answered: parsing overlaps indexing, and one object uses more than one shard at a time. */
    static final int BULKS_IN_FLIGHT = 4;
    static final int MAX_ERROR_BODY = 10 << 20;
    static final int MAX_NAME_PART = 100;
    private static final Pattern PLACEHOLDER = Pattern.compile("\\{([A-Za-z0-9_.]{1,64})}");
    private static final TimeValue BULK_TIMEOUT = TimeValue.timeValueMinutes(2);

    private final Client client;
    private final ThreadContext threadContext;
    private final boolean securityEnabled;
    private volatile String authorization;
    private volatile String target;
    private volatile boolean minimal;

    S3Indexer(Client client, ThreadContext threadContext, Settings settings) {
        this.client = client;
        this.threadContext = threadContext;
        this.target = S3HttpSettings.TARGET.get(settings);
        setMetadata(S3HttpSettings.METADATA.get(settings));
        this.securityEnabled = settings.getAsBoolean("xpack.security.enabled", true);
        setApiKey(settings);
    }

    /**
     * Reads s3.api_key. Secure settings are only readable while the node starts and during
     * POST _nodes/reload_secure_settings, which calls this again: a new or rotated key needs no restart.
     */
    void setApiKey(Settings settings) {
        try (SecureString key = S3HttpSettings.API_KEY.exists(settings) ? S3HttpSettings.API_KEY.get(settings) : null) {
            this.authorization = key == null ? null : "ApiKey " + key;
        }
        if (authorization == null && settings.getAsBoolean("xpack.security.enabled", true)) {
            logger.warn("{} is not set in the keystore; uploads get 503 until it is", S3HttpSettings.API_KEY.getKey());
        }
    }

    void setTarget(String target) {
        this.target = target;
    }

    void setMetadata(String metadata) {
        this.minimal = "minimal".equals(metadata);
    }

    /** An index request; the target's default pipeline, if any, runs on it (the plugin's logs-s3@default-pipeline on logs-s3.*-*). */
    IndexRequest newDoc(String index) {
        return new IndexRequest(index);
    }

    /** Blocking: runs on the generic pool, up to BULKS_IN_FLIGHT chunks at a time. */
    /** @param body the object, for an error document if it fails to decode; null when it is not at hand (multipart) */
    Result index(ObjectDecoder.Opened object, ObjectKey key, Request req, BytesReference body) {
        Counts c = new Counts();
        ArrayDeque<Sent> inFlight = new ArrayDeque<>();
        BulkRequest bulk = new BulkRequest();
        long chunkBytes = 0;
        ObjectDecoder.Records records = object.records();
        String record;
        try {
            while ((record = records.next()) != null) {
                c.lines++;
                Map<String, Object> src = minimal ? minimalSource(record, records.number(), key, req)
                    : toSource(record, records.number(), object, key, req);
                bulk.add(newDoc(resolveTarget(key)).id(docId(key, req, records.number())).opType(DocWriteRequest.OpType.CREATE).source(src));
                chunkBytes += record.length();
                if (bulk.numberOfActions() >= CHUNK_DOCS || chunkBytes >= CHUNK_BYTES) {
                    inFlight.add(submit(bulk));
                    if (inFlight.size() >= BULKS_IN_FLIGHT && await(inFlight.poll(), c) == false) {
                        drain(inFlight, c);
                        return c.result(true);
                    }
                    bulk = new BulkRequest();
                    chunkBytes = 0;
                }
            }
        } catch (IOException e) {
            // corrupt/truncated compression or JSON array: keep what decoded, store the body so it can be replayed
            addDecodeError(bulk, c, key, req, body, object.compression(), records.number(), e);
        } catch (RuntimeException e) {
            // not the object's fault (a multipart part could not be read back): fail the request, so the client
            // retries; first let what was sent finish, so the retry never races it
            drain(inFlight, c);
            throw e;
        }
        if (bulk.numberOfActions() > 0) {
            inFlight.add(submit(bulk));
        }
        return c.result(drain(inFlight, c) == false);
    }

    /** An object that cannot be decompressed at all (corrupt header): one error document holding the body. */
    Result undecodable(ObjectKey key, Request req, BytesReference body, IOException e) {
        Counts c = new Counts();
        BulkRequest bulk = new BulkRequest();
        addDecodeError(bulk, c, key, req, body, null, 0, e);
        return c.result(send(bulk, c) == false);
    }

    private void addDecodeError(BulkRequest bulk, Counts c, ObjectKey key, Request req, BytesReference body, String compression, int lineNo,
        IOException e) {
        logger.warn("{}: object [{}/{}] failed to decode after {} lines", S3HttpSettings.PLUGIN, key.bucket(), key.key(), lineNo, e);
        c.invalid++;
        String b64 = body != null && body.length() <= MAX_ERROR_BODY ? Base64.getEncoder().encodeToString(BytesReference.toBytes(body)) : null;
        Map<String, Object> src = errorSource("decode failed after line " + lineNo + ": " + e, compression, key, req, b64);
        bulk.add(newDoc(resolveTarget(key)).id(docId(key, req, -1)).opType(DocWriteRequest.OpType.CREATE).source(src));
    }

    private static final class Counts {
        int lines, indexed, duplicates, dropped, invalid;

        Result result(boolean retryable) {
            return new Result(lines, indexed, duplicates, dropped, invalid, retryable);
        }
    }

    /** A bulk that was sent and is not answered yet. */
    private record Sent(int docs, ActionFuture<BulkResponse> response) {}

    /**
     * Sends a bulk as the keystore API key without waiting for the answer. When Elasticsearch can't
     * authenticate the key (missing, revoked, expired, wrong secret) a request from inside the node runs
     * as the internal {@code _system} user instead of failing, which may write to any index: so each bulk
     * first proves the key authenticates ({@link KeyCheckAction}), and fails (the client retries) if not.
     */
    private Sent submit(BulkRequest bulk) {
        PlainActionFuture<BulkResponse> response = new PlainActionFuture<>();
        String auth = authorization;
        if (securityEnabled == false) {
            asKey(auth, () -> client.bulk(bulk, response));
        } else if (auth == null) {
            response.onFailure(new IllegalStateException(S3HttpSettings.API_KEY.getKey() + " is not set in the keystore"));
        } else {
            asKey(auth, () -> client.execute(KeyCheckAction.TYPE, new KeyCheckAction.Request(), ActionListener.wrap(r -> {
                if (r.apiKey) {
                    asKey(auth, () -> client.bulk(bulk, response));
                } else {
                    response.onFailure(new IllegalStateException(S3HttpSettings.API_KEY.getKey() + " can't be authenticated (revoked, expired or wrong)"));
                }
            }, e -> {
                // refused: only a real identity is refused a cluster:monitor action, and the only one sent is the key
                if (ExceptionsHelper.status(e) == RestStatus.FORBIDDEN) {
                    asKey(auth, () -> client.bulk(bulk, response));
                } else {
                    response.onFailure(e);
                }
            })));
        }
        return new Sent(bulk.numberOfActions(), response);
    }

    /** Runs {@code send} with only the API key in the thread context (none when security is off). */
    private void asKey(String auth, Runnable send) {
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            if (auth != null) {
                threadContext.putHeader("Authorization", auth);
            }
            send.run();
        }
    }

    /** Waits for every bulk still in flight, even after one failed, so a retry never races the attempt before it. @return false when the object must be retried */
    private boolean drain(ArrayDeque<Sent> inFlight, Counts c) {
        boolean ok = true;
        for (Sent s; (s = inFlight.poll()) != null; ) {
            ok &= await(s, c);
        }
        return ok;
    }

    private boolean send(BulkRequest bulk, Counts c) {
        return await(submit(bulk), c);
    }

    /** @return false when the object must be retried */
    private boolean await(Sent sent, Counts c) {
        BulkResponse resp;
        try {
            resp = sent.response().actionGet(BULK_TIMEOUT);
        } catch (Exception e) {
            logger.warn("{}: bulk of {} docs failed; the client will retry the object", S3HttpSettings.PLUGIN, sent.docs(), e);
            return false;
        }
        boolean retryable = false;
        BulkItemResponse.Failure first = null;
        for (BulkItemResponse item : resp.getItems()) {
            if (item.isFailed() == false) {
                c.indexed++;
                continue;
            }
            RestStatus st = item.getFailure().getStatus();
            if (st == RestStatus.CONFLICT) {
                c.duplicates++; // indexed by an earlier attempt of this object
            } else if (retryable(st)) {
                retryable = true;
                first = first == null ? item.getFailure() : first;
            } else {
                c.dropped++;
                first = first == null ? item.getFailure() : first;
            }
        }
        if (first != null) {
            logger.warn("{}: bulk item failures (retry={}), first: [{}] {}", S3HttpSettings.PLUGIN, retryable, first.getIndex(), first.getMessage());
        }
        return retryable == false;
    }

    /**
     * Item failures that are about the cluster, not the line, so the client should resend the object:
     * back-pressure, the API key lacking privileges, or the target deleted while the bulk ran (404:
     * the retry recreates the data stream). Anything else is the line's own problem and retrying
     * would fail the same way.
     */
    static boolean retryable(RestStatus st) {
        return st == RestStatus.TOO_MANY_REQUESTS || st == RestStatus.SERVICE_UNAVAILABLE
            || st == RestStatus.UNAUTHORIZED || st == RestStatus.FORBIDDEN || st == RestStatus.NOT_FOUND;
    }

    /** 20-char id from bucket, key, ETag and line number: stable across re-uploads of the same object. */
    static String docId(ObjectKey key, Request req, int line) {
        byte[] h = SigV4.sha256((key.bucket() + "/" + key.key() + "/" + req.etag() + "#" + line).getBytes(StandardCharsets.UTF_8));
        return Base64.getUrlEncoder().withoutPadding().encodeToString(java.util.Arrays.copyOf(h, 15));
    }

    /** The target with {bucket} and {partition name} filled in from the object key. */
    String resolveTarget(ObjectKey key) {
        String t = this.target;
        if (t.indexOf('{') < 0) {
            return t;
        }
        Matcher m = PLACEHOLDER.matcher(t);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String name = m.group(1);
            m.appendReplacement(out, Matcher.quoteReplacement(sanitize(name.equals("bucket") ? key.bucket() : key.partition().get(name))));
        }
        return m.appendTail(out).toString();
    }

    /** Data stream name parts: lowercase, no '-', no characters ES rejects. */
    static String sanitize(String v) {
        if (v == null || v.isEmpty()) {
            return "unknown";
        }
        String s = v.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_.]", "_");
        s = s.startsWith("_") || s.startsWith(".") ? "x" + s : s;
        // data stream names are limited to 255 bytes; a longer one fails every line, without a retry
        return s.length() > MAX_NAME_PART ? s.substring(0, MAX_NAME_PART) : s;
    }

    /**
     * One line as a document: the line itself in {@code message}, untouched (an ingest pipeline parses it),
     * when it arrived, and where it came from under {@code s3.*}.
     */
    static Map<String, Object> toSource(String line, int lineNo, ObjectDecoder.Opened object, ObjectKey key, Request req) {
        Map<String, Object> src = new HashMap<>();
        String received = Instant.ofEpochMilli(req.receivedMillis()).toString();
        src.put("@timestamp", received);
        src.put("message", line);
        Map<String, Object> ns = object(key, req, object.compression());
        ns.put("format", object.format());
        ns.put("line", lineNo);
        src.put(S3HttpSettings.NS, ns);
        putIfNotNull(src, "agent", agent(req.userAgent()));
        src.put("event", Map.of("module", S3HttpSettings.NS, "ingested", received));
        return src;
    }

    /** s3.metadata: minimal. Only what tells lines apart and who sent them. */
    static Map<String, Object> minimalSource(String line, int lineNo, ObjectKey key, Request req) {
        Map<String, Object> ns = new HashMap<>();
        ns.put("bucket", key.bucket());
        ns.put("key", key.key());
        ns.put("line", lineNo);
        if (req.key() != null) {
            ns.put("access_key_name", req.key().name());
        }
        Map<String, Object> src = new HashMap<>();
        src.put("@timestamp", Instant.ofEpochMilli(req.receivedMillis()).toString());
        src.put("message", line);
        src.put(S3HttpSettings.NS, ns);
        putIfNotNull(src, "agent", agent(req.userAgent()));
        src.put("event", Map.of("module", S3HttpSettings.NS));
        return src;
    }

    static Map<String, Object> errorSource(String error, String compression, ObjectKey key, Request req, String bodyBase64) {
        Map<String, Object> src = new HashMap<>();
        String received = Instant.ofEpochMilli(req.receivedMillis()).toString();
        src.put("@timestamp", received);
        Map<String, Object> ns = object(key, req, compression);
        putIfNotNull(ns, "body_base64", bodyBase64);
        src.put(S3HttpSettings.NS, ns);
        putIfNotNull(src, "agent", agent(req.userAgent()));
        src.put("error", Map.of("message", error, "type", S3HttpSettings.NS + "_decode"));
        src.put("event", Map.of("kind", "pipeline_error", "module", S3HttpSettings.NS, "ingested", received));
        return src;
    }

    /**
     * agent.type and agent.version: what sent the object, from the first product of its User-Agent
     * ("aws-cli/2.17.0 Python/3.11 ..." is aws-cli, 2.17.0). Null when the request had no User-Agent.
     */
    static Map<String, Object> agent(String userAgent) {
        String ua = userAgent == null ? "" : userAgent.strip();
        int slash = ua.indexOf('/');
        if (slash < 0) {
            String type = ua.split("\\s", 2)[0];
            return type.isEmpty() ? null : Map.of("type", type);
        }
        String type = ua.substring(0, slash).strip();
        if (type.isEmpty()) {
            return null;
        }
        int end = slash + 1;
        while (end < ua.length() && Character.isWhitespace(ua.charAt(end)) == false && ua.charAt(end) != ';' && ua.charAt(end) != ')') {
            end++;
        }
        String version = ua.substring(slash + 1, end);
        return version.isEmpty() ? Map.of("type", type) : Map.of("type", type, "version", version);
    }

    /** The object and the request it came in. */
    private static Map<String, Object> object(ObjectKey key, Request req, String compression) {
        Map<String, Object> ns = new HashMap<>();
        ns.put("bucket", key.bucket());
        ns.put("key", key.key());
        putIfNotNull(ns, "etag", req.etag());
        putIfNotNull(ns, "compression", compression);
        if (key.partition().isEmpty() == false) {
            ns.put("partition", key.partition());
        }
        putIfNotNull(ns, "access_key_name", req.key() == null ? null : req.key().name());
        putIfNotNull(ns, "access_key_id", req.key() == null ? null : req.key().id());
        Map<String, Object> r = new HashMap<>();
        r.put("id", req.id());
        r.put("received", Instant.ofEpochMilli(req.receivedMillis()).toString());
        putIfNotNull(r, "node", req.node());
        putIfNotNull(r, "remote_address", req.remoteAddress());
        putIfNotNull(r, "user_agent", req.userAgent());
        r.put("bytes", req.bytes());
        ns.put("request", r);
        return ns;
    }

    private static void putIfNotNull(Map<String, Object> m, String k, Object v) {
        if (v != null) {
            m.put(k, v);
        }
    }
}
