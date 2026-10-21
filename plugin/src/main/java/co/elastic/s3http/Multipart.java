package co.elastic.s3http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * S3 multipart uploads. A load balancer may send each part to a different node, so parts are kept in a
 * system index until CompleteMultipartUpload, which reads them back one at a time, in order, as one object
 * (never all of it in memory), indexes its lines like a PutObject, then deletes them. Uploads neither
 * completed nor aborted are deleted after {@code s3.multipart_expiry}. The index is written as the
 * plugin itself, not the API key: the key needs no privileges on it, and users can't read it.
 */
final class Multipart {

    private static final Logger logger = LogManager.getLogger(Multipart.class);

    static final String INDEX = "." + S3HttpSettings.NS + "-uploads-1";
    static final int MAX_PARTS = 10_000;
    private static final String ORIGIN = "stack";
    private static final TimeValue TIMEOUT = TimeValue.timeValueMinutes(1);
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final Pattern PART = Pattern.compile(
        "<Part>(?:(?!</Part>).)*?<PartNumber>\\s*(\\d{1,5})\\s*</PartNumber>(?:(?!</Part>).)*?</Part>", Pattern.DOTALL);
    private static final Pattern ETAG = Pattern.compile("<ETag>([^<]*)</ETag>");
    private static final Pattern ENTITY = Pattern.compile("&(quot|apos|amp|lt|gt|#[0-9]{1,7}|#[xX][0-9A-Fa-f]{1,6});");

    static SystemIndexDescriptor descriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern("." + S3HttpSettings.NS + "-uploads*")
            .setPrimaryIndex(INDEX)
            .setDescription("Parts of multipart uploads to " + S3HttpSettings.PLUGIN + ", until they are completed")
            .setSettings(Settings.builder().put("index.hidden", true).put("index.number_of_shards", 1).put("index.auto_expand_replicas", "0-1")
                .build())
            .setMappings(mappings())
            .setOrigin(ORIGIN)
            .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
            .build();
    }

    private static XContentBuilder mappings() {
        try {
            XContentBuilder b = XContentFactory.jsonBuilder().startObject();
            b.startObject("_meta").field(SystemIndexDescriptor.VERSION_META_KEY, 1).endObject();
            b.field("dynamic", "strict");
            // the bytes are a stored field only: kept once, not again in _source
            b.startObject("_source").array("excludes", "data").endObject();
            b.startObject("properties");
            b.startObject("upload_id").field("type", "keyword").endObject();
            b.startObject("bucket").field("type", "keyword").endObject();
            b.startObject("key").field("type", "keyword").field("index", false).field("doc_values", false).endObject();
            b.startObject("access_key_id").field("type", "keyword").endObject();
            b.startObject("created").field("type", "date").endObject();
            b.startObject("part").field("type", "integer").endObject();
            b.startObject("etag").field("type", "keyword").field("index", false).field("doc_values", false).endObject();
            b.startObject("completed_etag").field("type", "keyword").field("index", false).field("doc_values", false).endObject();
            b.startObject("bytes").field("type", "long").endObject();
            b.startObject("data").field("type", "binary").field("store", true).endObject();
            return b.endObject().endObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A part named in CompleteMultipartUpload. */
    record Part(int number, String etag) {}

    /** A completed upload: its parts in order, read lazily, and the ETag S3 would give it. */
    record Assembled(String etag, long bytes, List<Part> parts) {}

    private final Client client;

    Multipart(Client client) {
        this.client = new OriginSettingClient(client, ORIGIN);
    }

    static String newUploadId() {
        byte[] b = new byte[24];
        RANDOM.nextBytes(b);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    /** CreateMultipartUpload: remembers who may add parts to it, and for which object. */
    String create(ObjectKey key, AccessKeys.Key accessKey) throws IOException {
        String id = newUploadId();
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
            .field("upload_id", id).field("bucket", key.bucket()).field("key", key.key())
            .field("access_key_id", accessKey.id()).field("created", Instant.now().toString()).endObject();
        client.index(new IndexRequest(INDEX).id(id).opType(DocWriteRequest.OpType.CREATE).source(doc)).actionGet(TIMEOUT);
        return id;
    }

    /** An upload looked up: an error, or found; {@code completedEtag} is set once it was completed. */
    record Found(S3Error error, String completedEtag) {}

    /** The upload, if it exists and is for this object and access key; otherwise a NoSuchUpload error. */
    Found find(String uploadId, ObjectKey key, AccessKeys.Key accessKey) {
        if (uploadId == null || uploadId.isBlank() || uploadId.length() > 64) {
            return new Found(noSuchUpload(), null);
        }
        GetResponse r;
        try {
            r = client.get(new GetRequest(INDEX, uploadId)).actionGet(TIMEOUT);
        } catch (IndexNotFoundException e) {
            return new Found(noSuchUpload(), null);
        }
        if (r.isExists() == false) {
            return new Found(noSuchUpload(), null);
        }
        Map<String, Object> src = r.getSourceAsMap();
        if (key.bucket().equals(src.get("bucket")) == false || key.key().equals(src.get("key")) == false
            || accessKey.id().equals(src.get("access_key_id")) == false) {
            return new Found(noSuchUpload(), null);
        }
        return new Found(null, (String) src.get("completed_etag"));
    }

    /** For UploadPart and AbortMultipartUpload: a completed upload no longer exists, as in S3. */
    S3Error check(String uploadId, ObjectKey key, AccessKeys.Key accessKey) {
        Found f = find(uploadId, key, accessKey);
        return f.error() != null ? f.error() : f.completedEtag() != null ? noSuchUpload() : null;
    }

    /** UploadPart: stores (or replaces) one part. */
    void putPart(String uploadId, int number, BytesReference data, String etag) throws IOException {
        // the request's own buffer when it is one piece (the usual case), so the builder's is the only copy
        BytesRef bytes = data.toBytesRef();
        XContentBuilder doc = XContentFactory.smileBuilder().startObject()
            .field("upload_id", uploadId).field("part", number).field("etag", etag).field("bytes", data.length())
            .field("created", Instant.now().toString())
            .field("data", bytes.bytes, bytes.offset, bytes.length)
            .endObject();
        client.index(new IndexRequest(INDEX).id(partId(uploadId, number)).source(doc)).actionGet(TIMEOUT);
    }

    /** CompleteMultipartUpload's list of parts, from its XML body. */
    static List<Part> parseCompletion(String xml) {
        List<Part> parts = new ArrayList<>();
        Matcher m = PART.matcher(xml);
        while (m.find()) {
            Matcher e = ETAG.matcher(m.group());
            String etag = e.find() ? unescape(e.group(1)).replace("\"", "").trim().toLowerCase(java.util.Locale.ROOT) : null;
            parts.add(new Part(Integer.parseInt(m.group(1)), etag));
        }
        return parts;
    }

    /** XML character references: clients quote the ETag as &quot;, &#34; or a bare quote. */
    static String unescape(String text) {
        return ENTITY.matcher(text).replaceAll(r -> {
            String e = r.group(1);
            String c = switch (e) {
                case "quot" -> "\"";
                case "apos" -> "'";
                case "amp" -> "&";
                case "lt" -> "<";
                case "gt" -> ">";
                default -> Character.toString(e.charAt(1) == 'x' || e.charAt(1) == 'X'
                    ? Integer.parseInt(e.substring(2), 16) : Integer.parseInt(e.substring(1)));
            };
            return Matcher.quoteReplacement(c);
        });
    }

    /** Checks every listed part is stored with the ETag the client was given; an S3 error if not. */
    Object assemble(String uploadId, List<Part> listed) {
        if (listed.isEmpty()) {
            return new S3Error(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.");
        }
        MessageDigest md5s = md5();
        long bytes = 0;
        int previous = 0;
        for (Part p : listed) {
            if (p.number() <= previous || p.number() > MAX_PARTS) {
                return new S3Error(400, "InvalidPartOrder", "The list of parts was not in ascending order.");
            }
            previous = p.number();
            GetResponse r = client.get(new GetRequest(INDEX, partId(uploadId, p.number()))).actionGet(TIMEOUT);
            Map<String, Object> src = r.isExists() ? r.getSourceAsMap() : null;
            if (src == null || p.etag() == null || p.etag().equals(src.get("etag")) == false) {
                return new S3Error(400, "InvalidPart",
                    "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.");
            }
            md5s.update(HexFormat.of().parseHex(p.etag()));
            bytes += ((Number) src.get("bytes")).longValue();
        }
        return new Assembled(HexFormat.of().formatHex(md5s.digest()) + "-" + listed.size(), bytes, listed);
    }

    /** The parts as one stream, each read from the index only when the one before it is used up. */
    InputStream open(String uploadId, List<Part> parts) {
        return concat(parts, n -> load(uploadId, n));
    }

    static InputStream concat(List<Part> parts, java.util.function.IntFunction<BytesReference> load) {
        return new InputStream() {
            private int next;
            private InputStream current = InputStream.nullInputStream();

            @Override
            public int read() throws IOException {
                byte[] one = new byte[1];
                return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                while (true) {
                    int n = current.read(b, off, len);
                    if (n >= 0 || next >= parts.size()) {
                        return n;
                    }
                    current = load.apply(parts.get(next++).number()).streamInput();
                }
            }
        };
    }

    /** One part's bytes; a part that can't be read is {@link ObjectDecoder.PartUnavailable}: the request fails, the parts stay. */
    private BytesReference load(String uploadId, int number) {
        GetResponse r;
        try {
            r = client.get(new GetRequest(INDEX, partId(uploadId, number)).storedFields("data")).actionGet(TIMEOUT);
        } catch (RuntimeException e) {
            throw new ObjectDecoder.PartUnavailable("part " + number + " of upload " + uploadId + " could not be read", e);
        }
        DocumentField f = r.isExists() ? r.getField("data") : null;
        if (f == null || f.getValue() == null) {
            // e.g. a retried Complete running alongside this one already finished and deleted it
            throw new ObjectDecoder.PartUnavailable("part " + number + " of upload " + uploadId + " is gone", null);
        }
        Object v = f.getValue();
        if (v instanceof BytesReference br) {
            return br;
        }
        if (v instanceof BytesRef ref) {
            return new BytesArray(ref);
        }
        if (v instanceof byte[] a) {
            return new BytesArray(a);
        }
        return new BytesArray(Base64.getDecoder().decode(Objects.toString(v)));
    }

    /**
     * After a completed upload is indexed: its parts go, and its record stays, marked completed with its ETag,
     * until it expires, so a retried Complete (the client never saw the first answer) gets the same 200 instead
     * of uploading the whole object again.
     */
    void completed(String uploadId, List<Part> parts, String etag) {
        BulkRequest bulk = new BulkRequest();
        bulk.add(new UpdateRequest(INDEX, uploadId).doc(Map.of("completed_etag", etag)));
        for (Part p : parts) {
            bulk.add(new DeleteRequest(INDEX, partId(uploadId, p.number())));
        }
        client.bulk(bulk).actionGet(TIMEOUT);
        // parts uploaded but not listed
        deleteWhere(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("upload_id", uploadId)).filter(QueryBuilders.existsQuery("part")));
    }

    /** AbortMultipartUpload. */
    void abort(String uploadId) {
        deleteWhere(QueryBuilders.termQuery("upload_id", uploadId));
    }

    /** Uploads older than {@code age}, and their parts. */
    void expire(TimeValue age) {
        deleteWhere(QueryBuilders.rangeQuery("created").lt(Instant.now().minusMillis(age.millis()).toString()));
    }

    private void deleteWhere(org.elasticsearch.index.query.QueryBuilder query) {
        try {
            client.admin().indices().refresh(new RefreshRequest(INDEX)).actionGet(TIMEOUT);
            DeleteByQueryRequest dbq = new DeleteByQueryRequest(INDEX).setQuery(query).setAbortOnVersionConflict(false);
            BulkByPaginatedSearchResponse r = client.execute(DeleteByQueryAction.INSTANCE, dbq).actionGet(TIMEOUT);
            if (r.getBulkFailures().isEmpty() == false || r.getSearchFailures().isEmpty() == false) {
                logger.warn("{}: deleting multipart uploads: {} deleted, failures {} {}", S3HttpSettings.PLUGIN, r.getDeleted(),
                    r.getBulkFailures(), r.getSearchFailures());
            }
        } catch (IndexNotFoundException e) {
            // nothing was ever uploaded in parts
        } catch (RuntimeException e) {
            logger.warn("{}: deleting multipart uploads failed", S3HttpSettings.PLUGIN, e);
        }
    }

    static String partId(String uploadId, int number) {
        return uploadId + "." + number;
    }

    private static S3Error noSuchUpload() {
        return new S3Error(404, "NoSuchUpload",
            "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.");
    }

    private static MessageDigest md5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
