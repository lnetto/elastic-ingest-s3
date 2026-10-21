package co.elastic.s3http;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.rest.RestHandler;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * An S3-compatible upload endpoint on the Elasticsearch HTTP port: anything that can write ndjson to an
 * S3 bucket can point at it instead, and every line is indexed rather than stored.
 */
public class S3HttpPlugin extends Plugin implements ActionPlugin, ReloadablePlugin, SystemIndexPlugin {

    private final SetOnce<S3Service> service = new SetOnce<>();
    private final SetOnce<S3TemplateRegistry> templates = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        return S3HttpSettings.all();
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        Settings settings = services.environment().settings();
        S3Indexer indexer = new S3Indexer(services.client(), services.threadPool().getThreadContext(), settings);
        Multipart multipart = new Multipart(services.client());
        S3Service svc = new S3Service(settings, services.threadPool(), indexer, multipart);
        ClusterSettings cs = services.clusterService().getClusterSettings();
        cs.addSettingsUpdateConsumer(S3HttpSettings.TARGET, indexer::setTarget);
        cs.addSettingsUpdateConsumer(S3HttpSettings.METADATA, indexer::setMetadata);
        cs.addSettingsUpdateConsumer(S3HttpSettings.ACCESS_KEYS, svc.accessKeys::set, AccessKeys::validate);
        cs.addSettingsUpdateConsumer(S3HttpSettings.BUCKETS, svc::setBuckets);
        cs.addSettingsUpdateConsumer(S3HttpSettings.RETRY_DELAY, svc::setRetryDelay);
        TimeValue[] expiry = {S3HttpSettings.MULTIPART_EXPIRY.get(settings)};
        cs.addSettingsUpdateConsumer(S3HttpSettings.MULTIPART_EXPIRY, v -> expiry[0] = v);
        // abandoned multipart uploads: the elected master deletes them, hourly
        services.threadPool().scheduleWithFixedDelay(() -> {
            if (services.clusterService().state().nodes().isLocalNodeElectedMaster()) {
                multipart.expire(expiry[0]);
            }
        }, TimeValue.timeValueHours(1), services.threadPool().generic());
        service.set(svc);
        S3TemplateRegistry registry = new S3TemplateRegistry(settings, services.clusterService(), services.threadPool(),
            services.client(), services.xContentRegistry(), services.featureService());
        registry.setEnabled(S3HttpSettings.TEMPLATES_ENABLED.get(settings));
        registry.initialize();
        templates.set(registry);
        return List.of();
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new S3RestHandler(service::get));
    }

    @Override
    public Collection<ActionHandler> getActions() {
        return List.of(new ActionHandler(KeyCheckAction.TYPE, KeyCheckAction.class));
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(Multipart.descriptor());
    }

    @Override
    public String getFeatureName() {
        return S3HttpSettings.PLUGIN;
    }

    @Override
    public String getFeatureDescription() {
        return "Multipart uploads to " + S3HttpSettings.PLUGIN + " that are not completed yet";
    }

    @Override
    public void close() {
        if (templates.get() != null) {
            templates.get().close();
        }
    }

    /** POST _nodes/reload_secure_settings: picks up a new or rotated s3.api_key without a restart. */
    @Override
    public void reload(Settings settings) {
        S3Service svc = service.get();
        if (svc != null) {
            svc.indexer.setApiKey(settings);
        }
    }
}
