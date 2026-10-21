package co.elastic.s3http;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

/** Shared state for the REST handler: accepted access keys and buckets, the indexer, multipart uploads, the executor. */
final class S3Service {

    final AccessKeys accessKeys = new AccessKeys();
    final S3Indexer indexer;
    final Multipart multipart;
    private final ThreadPool threadPool;
    private final String nodeName;
    private volatile Set<String> buckets;
    private volatile TimeValue retryDelay;

    S3Service(Settings settings, ThreadPool threadPool, S3Indexer indexer, Multipart multipart) {
        this.indexer = indexer;
        this.multipart = multipart;
        this.threadPool = threadPool;
        this.nodeName = settings.get("node.name");
        accessKeys.set(S3HttpSettings.ACCESS_KEYS.get(settings));
        setBuckets(S3HttpSettings.BUCKETS.get(settings));
        this.retryDelay = S3HttpSettings.RETRY_DELAY.get(settings);
    }

    void setRetryDelay(TimeValue retryDelay) {
        this.retryDelay = retryDelay;
    }

    /** Runs {@code r} after s3.retry_delay, without holding a thread meanwhile. */
    void afterRetryDelay(Runnable r) {
        TimeValue d = retryDelay;
        if (d.millis() <= 0) {
            r.run();
        } else {
            threadPool.schedule(r, d, threadPool.generic());
        }
    }

    void setBuckets(List<String> buckets) {
        this.buckets = Set.copyOf(buckets);
    }

    boolean bucketAllowed(String bucket) {
        Set<String> b = buckets;
        return b.isEmpty() || b.contains(bucket);
    }

    /** Decoding, the (blocking) bulk calls and multipart bookkeeping run here, never on a network thread. */
    Executor generic() {
        return threadPool.generic();
    }

    String nodeName() {
        return nodeName;
    }
}
