package co.elastic.s3http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Who a bulk sent with the keystore API key would run as. When Elasticsearch can't authenticate that key
 * (revoked, expired, wrong secret, missing) a request from inside the node runs as the internal
 * {@code _system} user instead of failing, and {@code _system} may write to any existing index: the key's
 * privileges would no longer limit anything. So every bulk is preceded by this action, sent the same way.
 * It is a {@code cluster:monitor} action, which {@code _system} may run: a key without cluster privileges
 * (as the README creates it) is refused with 403, which proves the key itself was authenticated. If it
 * runs, the caller is {@code _system}, or a key with cluster privileges: the response says which.
 */
public final class KeyCheckAction extends TransportAction<KeyCheckAction.Request, KeyCheckAction.Response> {

    static final String NAME = "cluster:monitor/" + S3HttpSettings.NS + "/key_check";
    static final ActionType<Response> TYPE = new ActionType<>(NAME);

    /** Thread-context transient holding the caller's authentication (x-pack security). */
    private static final String AUTHENTICATION = "_xpack_security_authentication";

    private final ThreadPool threadPool;

    @Inject
    public KeyCheckAction(ActionFilters filters, TransportService transport, ThreadPool threadPool) {
        super(NAME, filters, transport.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        Object auth = threadPool.getThreadContext().getTransient(AUTHENTICATION);
        listener.onResponse(new Response(isApiKey(auth)));
    }

    /** True only for an API key's authentication; the internal user's (or none, security off) is not. */
    static boolean isApiKey(Object authentication) {
        if (authentication == null) {
            return false;
        }
        try {
            return (boolean) authentication.getClass().getMethod("isApiKey").invoke(authentication);
        } catch (ReflectiveOperationException | ClassCastException e) {
            return false;
        }
    }

    static final class Request extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) {
            TransportAction.localOnly();
        }
    }

    static final class Response extends ActionResponse {
        final boolean apiKey;

        Response(boolean apiKey) {
            this.apiKey = apiKey;
        }

        @Override
        public void writeTo(StreamOutput out) {
            TransportAction.localOnly();
        }
    }
}
