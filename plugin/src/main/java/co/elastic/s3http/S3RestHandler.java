package co.elastic.s3http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * S3 PutObject and multipart uploads on the Elasticsearch HTTP port, path-style ({@code /<bucket>/<key...>}).
 * Requests arrive as the privilege-less anonymous user (see {@link S3SecurityExtension}); the SigV4 signature
 * is verified here against {@code s3.access_keys}, then the object's lines are indexed as the plugin's
 * API key. An object is answered only once every line is indexed: a 200 means it is stored.
 */
final class S3RestHandler implements RestHandler {

    private static final Logger logger = LogManager.getLogger(S3RestHandler.class);

    /** Deepest key accepted: a prefix, a few name=value partitions and the file leave plenty of room. */
    static final int MAX_DEPTH = 16;
    /** Largest CompleteMultipartUpload body read: 10,000 parts listed with checksums is about 2 MB. */
    private static final int MAX_COMPLETION_XML = 4 << 20;
    private static final String S3_NS = "xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"";

    private final Supplier<S3Service> service;

    S3RestHandler(Supplier<S3Service> service) {
        this.service = service;
    }

    /**
     * One route per key depth and method: ES path patterns match a single segment per wildcard. The first
     * segment is named {index} because Elasticsearch's own /{index}/... routes share that trie node, and
     * literal segments (/_doc, /_mapping, ...) always win over these wildcards.
     */
    @Override
    public List<Route> routes() {
        List<Route> routes = new ArrayList<>();
        StringBuilder path = new StringBuilder("/{index}");
        for (int d = 1; d < MAX_DEPTH; d++) {
            path.append("/{s3_k").append(d).append('}');
            for (RestRequest.Method m : List.of(RestRequest.Method.PUT, RestRequest.Method.POST, RestRequest.Method.DELETE)) {
                routes.add(new Route(m, path.toString()));
            }
        }
        return routes;
    }

    /** S3 clients send application/octet-stream or nothing, which ES would otherwise refuse with 406. */
    @Override
    public boolean mediaTypesValid(RestRequest request) {
        return true;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return true;
    }

    /** What the request asks for: decided by method and query, as S3 does. */
    enum Op { PUT_OBJECT, CREATE_UPLOAD, UPLOAD_PART, COMPLETE_UPLOAD, ABORT_UPLOAD }

    static Op op(RestRequest.Method method, Map<String, String> params) {
        // AWS SDKs for Go and Java name the operation in x-id (?x-id=PutObject): only a hint, S3 ignores it
        Map<String, String> query = new HashMap<>(params);
        query.remove("x-id");
        boolean upload = query.containsKey("uploadId");
        return switch (method) {
            case PUT -> upload && query.containsKey("partNumber") ? Op.UPLOAD_PART : query.isEmpty() ? Op.PUT_OBJECT : null;
            case POST -> query.containsKey("uploads") && upload == false ? Op.CREATE_UPLOAD : upload ? Op.COMPLETE_UPLOAD : null;
            case DELETE -> upload ? Op.ABORT_UPLOAD : null;
            default -> null;
        };
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
        S3Service svc = service.get();
        String requestId = UUID.randomUUID().toString();
        String uri = request.uri();
        int q = uri.indexOf('?');
        String rawPath = q < 0 ? uri : uri.substring(0, q);
        String rawQuery = q < 0 ? "" : uri.substring(q + 1);
        Map<String, String> query = parseQuery(rawQuery);
        Op op = op(request.method(), query);
        if (op == null) {
            // ?tagging, ?acl, GET, DeleteObject ...: this endpoint only takes uploads
            send(channel, new S3Error(501, "NotImplemented", "Only PutObject and multipart uploads are supported"), rawPath, requestId);
            return;
        }
        Map<String, String> headers = new HashMap<>();
        request.getHeaders().forEach((k, v) -> headers.put(k.toLowerCase(Locale.ROOT), String.join(",", v)));
        // The network buffer itself, never a copy: Elasticsearch's in-flight requests circuit breaker counts it
        // until the response is sent, while a copy would be uncounted heap. Many large uploads at once (or
        // unsigned ones from anyone who can reach the port) would otherwise run the node out of memory, which
        // is fatal. Retained here because the work outlives this call; released once it is done.
        ReleasableBytesReference body = request.hasContent() ? request.content().retain() : ReleasableBytesReference.wrap(BytesArray.EMPTY);
        String method = request.method().name();
        String remote = remote(request);
        long received = System.currentTimeMillis();
        try {
            // hashing, checksums and decoding take time per byte: never on a network thread
            svc.generic().execute(() -> {
                try (body) {
                    handle(svc, channel, op, method, rawPath, rawQuery, query, headers, body, requestId, remote, received);
                } catch (Exception e) {
                    logger.warn("{}: {} [{}] failed", S3HttpSettings.PLUGIN, op, rawPath, e);
                    failLater(svc, channel, ExceptionsHelper.status(e) == RestStatus.TOO_MANY_REQUESTS
                        ? new S3Error(503, "SlowDown", "Please reduce your request rate.")
                        : new S3Error(500, "InternalError", "We encountered an internal error. Please try again."), rawPath, requestId);
                }
            });
        } catch (RuntimeException e) {
            body.decRef(); // the executor refused it (node shutting down)
            throw e;
        }
    }

    private void handle(S3Service svc, RestChannel channel, Op op, String method, String rawPath, String rawQuery, Map<String, String> query,
        Map<String, String> headers, BytesReference body, String requestId, String remote, long received) throws IOException {
        SigV4.Result auth = SigV4.verify(method, rawPath, rawQuery, headers, body, svc.accessKeys, Instant.now());
        if (auth.error() != null) {
            logger.debug("{}: rejected [{}]: {}", S3HttpSettings.PLUGIN, rawPath, auth.error().code());
            // a known key with a wrong signature is a sender that will retry at once: slow it down; anything else
            // (no signature, an unknown key) is answered now, so junk never holds its body for retry_delay
            if (auth.error().code().equals("SignatureDoesNotMatch")) {
                failLater(svc, channel, auth.error(), rawPath, requestId);
            } else {
                send(channel, auth.error(), rawPath, requestId);
            }
            return;
        }
        List<String> segments = new ArrayList<>();
        for (String s : rawPath.substring(1).split("/")) {
            segments.add(URLDecoder.decode(s.replace("+", "%2B"), StandardCharsets.UTF_8));
        }
        if (svc.bucketAllowed(segments.get(0)) == false) {
            send(channel, new S3Error(404, "NoSuchBucket", "The specified bucket does not exist"), rawPath, requestId);
            return;
        }
        ObjectKey key = ObjectKey.parse(segments);
        BytesReference payload = auth.payload();
        String etag = null;
        if (op == Op.PUT_OBJECT || op == Op.UPLOAD_PART) {
            byte[] md5 = SigV4.digest("MD5", payload);
            S3Error bad = Checksums.verify(headers, auth.trailers(), payload, md5);
            if (bad != null) {
                send(channel, bad, rawPath, requestId);
                return;
            }
            etag = HexFormat.of().formatHex(md5);
        }
        String userAgent = headers.get("user-agent");
        switch (op) {
            case PUT_OBJECT -> putObject(svc, channel, rawPath, requestId, key, payload,
                new S3Indexer.Request(requestId, received, svc.nodeName(), remote, auth.key(), payload.length(), userAgent, etag));
            case CREATE_UPLOAD -> {
                String id = svc.multipart.create(key, auth.key());
                sendXml(channel, requestId, "<InitiateMultipartUploadResult " + S3_NS + "><Bucket>" + esc(key.bucket()) + "</Bucket><Key>"
                    + esc(key.key()) + "</Key><UploadId>" + id + "</UploadId></InitiateMultipartUploadResult>", Map.of());
            }
            case UPLOAD_PART -> {
                int part;
                try {
                    part = Integer.parseInt(query.get("partNumber"));
                } catch (NumberFormatException e) {
                    part = 0;
                }
                if (part < 1 || part > Multipart.MAX_PARTS) {
                    send(channel, new S3Error(400, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive"), rawPath,
                        requestId);
                    return;
                }
                String id = query.get("uploadId");
                S3Error e = svc.multipart.check(id, key, auth.key());
                if (e != null) {
                    send(channel, e, rawPath, requestId);
                    return;
                }
                svc.multipart.putPart(id, part, payload, etag);
                sendXml(channel, requestId, "", Map.of("ETag", "\"" + etag + "\""));
            }
            case COMPLETE_UPLOAD -> complete(svc, channel, rawPath, requestId, key, query.get("uploadId"), payload,
                new S3Indexer.Request(requestId, received, svc.nodeName(), remote, auth.key(), 0, userAgent, null));
            case ABORT_UPLOAD -> {
                String id = query.get("uploadId");
                S3Error e = svc.multipart.check(id, key, auth.key());
                if (e != null) {
                    send(channel, e, rawPath, requestId);
                    return;
                }
                svc.multipart.abort(id);
                RestResponse ok = new RestResponse(RestStatus.NO_CONTENT, "application/xml", "");
                ok.addHeader("x-amz-request-id", requestId);
                channel.sendResponse(ok);
            }
        }
    }

    private static void putObject(S3Service svc, RestChannel channel, String rawPath, String requestId, ObjectKey key, BytesReference payload,
        S3Indexer.Request req) {
        S3Indexer.Result r;
        try {
            r = svc.indexer.index(ObjectDecoder.open(payload.streamInput(), payload.length(), key.jsonFile()), key, req, payload);
        } catch (IOException e) {
            // the compression header itself is corrupt: retrying would fail the same way forever
            r = svc.indexer.undecodable(key, req, payload, e);
        }
        answer(svc, channel, rawPath, requestId, key, r, req.etag(), false);
    }

    private static void complete(S3Service svc, RestChannel channel, String rawPath, String requestId, ObjectKey key, String uploadId,
        BytesReference xml, S3Indexer.Request partial) {
        Multipart.Found found = svc.multipart.find(uploadId, key, partial.key());
        if (found.error() != null) {
            send(channel, found.error(), rawPath, requestId);
            return;
        }
        if (found.completedEtag() != null) {
            // a retry of a Complete that already succeeded (its answer was lost): the same answer again
            answer(svc, channel, rawPath, requestId, key, new S3Indexer.Result(0, 0, 0, 0, 0, false), found.completedEtag(), true);
            return;
        }
        if (xml.length() > MAX_COMPLETION_XML) {
            send(channel, new S3Error(400, "MalformedXML", "The XML you provided was too large."), rawPath, requestId);
            return;
        }
        List<Multipart.Part> parts = Multipart.parseCompletion(xml.utf8ToString());
        Object assembled = svc.multipart.assemble(uploadId, parts);
        if (assembled instanceof S3Error err) {
            send(channel, err, rawPath, requestId);
            return;
        }
        Multipart.Assembled a = (Multipart.Assembled) assembled;
        S3Indexer.Request req = new S3Indexer.Request(partial.id(), partial.receivedMillis(), partial.node(), partial.remoteAddress(),
            partial.key(), a.bytes(), partial.userAgent(), a.etag());
        S3Indexer.Result r;
        try {
            r = svc.indexer.index(ObjectDecoder.open(svc.multipart.open(uploadId, a.parts()), a.bytes(), key.jsonFile()), key, req, null);
        } catch (IOException ex) {
            r = svc.indexer.undecodable(key, req, null, ex);
        }
        if (r.retryable() == false) {
            svc.multipart.completed(uploadId, parts, a.etag());
        }
        answer(svc, channel, rawPath, requestId, key, r, a.etag(), true);
    }

    private static void answer(S3Service svc, RestChannel channel, String rawPath, String requestId, ObjectKey key, S3Indexer.Result r,
        String etag, boolean multipart) {
        logger.debug("{}: [{}] {} lines, {} indexed, {} duplicates, {} dropped, {} invalid, retry={}", S3HttpSettings.PLUGIN,
            key.key(), r.lines(), r.indexed(), r.duplicates(), r.dropped(), r.invalid(), r.retryable());
        if (r.retryable()) {
            failLater(svc, channel, new S3Error(503, "SlowDown", "Please reduce your request rate."), rawPath, requestId);
        } else if (multipart == false) {
            sendXml(channel, requestId, "", Map.of("ETag", "\"" + etag + "\""));
        } else {
            sendXml(channel, requestId, "<CompleteMultipartUploadResult " + S3_NS + "><Location>" + esc(rawPath) + "</Location><Bucket>"
                + esc(key.bucket()) + "</Bucket><Key>" + esc(key.key()) + "</Key><ETag>&quot;" + etag
                + "&quot;</ETag></CompleteMultipartUploadResult>", Map.of());
        }
    }

    private static void sendXml(RestChannel channel, String requestId, String xml, Map<String, String> headers) {
        RestResponse ok = new RestResponse(RestStatus.OK, "application/xml",
            xml.isEmpty() ? "" : "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + xml);
        headers.forEach(ok::addHeader);
        ok.addHeader("x-amz-request-id", requestId);
        channel.sendResponse(ok);
    }

    /** Query parameters by name, decoded; a name without '=' maps to "". */
    static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> out = new HashMap<>();
        if (rawQuery.isEmpty() == false) {
            for (String p : rawQuery.split("&")) {
                int eq = p.indexOf('=');
                String name = URLDecoder.decode(eq < 0 ? p : p.substring(0, eq), StandardCharsets.UTF_8);
                out.put(name, eq < 0 ? "" : URLDecoder.decode(p.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    private static String esc(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }

    /**
     * Answers after s3.retry_delay: a sender told to retry (503, 500) or holding a wrong secret for a known
     * key may retry at once, with no backoff of its own (hundreds of attempts a second), so answering late is the
     * only backoff it gets. The wait is a scheduled task; no thread is held. Only these: an answer held back
     * keeps the request's body in memory meanwhile, so requests anyone could send are answered at once.
     */
    private static void failLater(S3Service svc, RestChannel channel, S3Error e, String resource, String requestId) {
        svc.afterRetryDelay(() -> send(channel, e, resource, requestId));
    }

    private static void send(RestChannel channel, S3Error e, String resource, String requestId) {
        RestResponse resp = new RestResponse(RestStatus.fromCode(e.status()), "application/xml", e.xml(resource, requestId));
        resp.addHeader("x-amz-request-id", requestId);
        channel.sendResponse(resp);
    }

    /**
     * Client address; behind the Elastic Cloud proxy that is X-Forwarded-For. Its last entry, the one the proxy
     * added: the client can put anything in the ones before.
     */
    private static String remote(RestRequest request) {
        String xff = request.header("X-Forwarded-For");
        if (xff != null && xff.isBlank() == false) {
            return xff.substring(xff.lastIndexOf(',') + 1).trim();
        }
        if (request.getHttpChannel() != null && request.getHttpChannel().getRemoteAddress() instanceof InetSocketAddress a) {
            return a.getAddress().getHostAddress();
        }
        return null;
    }
}
