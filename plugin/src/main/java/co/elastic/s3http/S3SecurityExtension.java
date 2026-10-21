package co.elastic.s3http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;

import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Defines the role {@code s3_anonymous} with no privileges at all. S3 clients sign with AWS SigV4
 * ({@code Authorization: AWS4-HMAC-SHA256 ...}), which no Elasticsearch realm understands, so requests
 * arrive as the anonymous user (xpack.security.authc.anonymous.roles: s3_anonymous) and the plugin
 * verifies the signature itself, then indexes as its own API key.
 */
public final class S3SecurityExtension implements SecurityExtension {

    static final String ANONYMOUS_ROLE = S3HttpSettings.NS + "_anonymous";

    private static final RoleDescriptor EMPTY = new RoleDescriptor(ANONYMOUS_ROLE, new String[0], new RoleDescriptor.IndicesPrivileges[0], null);

    @Override
    public String extensionName() {
        return S3HttpSettings.PLUGIN;
    }

    @Override
    public List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> getRolesProviders(SecurityComponents components) {
        return List.of((names, listener) -> listener.onResponse(RoleRetrievalResult.success(names.contains(ANONYMOUS_ROLE) ? Set.of(EMPTY) : Set.of())));
    }
}
