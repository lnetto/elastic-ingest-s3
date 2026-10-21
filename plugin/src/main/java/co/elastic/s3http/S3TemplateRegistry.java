package co.elastic.s3http;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.YamlTemplateRegistry;

/**
 * Installs the index template for the plugin's data streams, logs-s3.*-*, the way Elasticsearch's own
 * inputs (Prometheus remote write, OTLP) do: the elected master puts it, and puts it again when resources.yaml's
 * version goes up. It also installs logs-s3@default-pipeline, the streams' default pipeline (set in logs-s3@settings),
 * which calls the logs-s3@custom ingest pipeline, then logs-<dataset>@custom for the stream's own dataset. The template includes logs@custom and logs-s3@custom, the
 * component templates that are yours: settings and mappings for these streams go there.
 */
final class S3TemplateRegistry extends YamlTemplateRegistry {

    S3TemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry,
        FeatureService featureService
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry, featureService);
    }

    @Override
    public void initialize() {
        super.initialize();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(S3HttpSettings.TEMPLATES_ENABLED, this::setEnabled);
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.STACK_ORIGIN;
    }

    @Override
    public String getName() {
        return S3HttpSettings.PLUGIN;
    }

    @Override
    protected String getVersionProperty() {
        return S3HttpSettings.NS + ".template.version";
    }
}
