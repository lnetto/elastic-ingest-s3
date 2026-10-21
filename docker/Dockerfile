# Elasticsearch with the plugin baked in.
#   docker build -f docker/Dockerfile -t elasticsearch-s3:9.5.4 .
# Pass the plugin's API key as -e S3_API_KEY=<encoded>; the entrypoint puts it in the keystore.
ARG STACK_VERSION=9.5.4
FROM docker.elastic.co/elasticsearch/elasticsearch:${STACK_VERSION}
ARG STACK_VERSION
COPY --chown=elasticsearch:root dist/ingest-s3-*-es${STACK_VERSION}.zip /tmp/ingest-s3.zip
RUN bin/elasticsearch-plugin install --batch file:///tmp/ingest-s3.zip && rm /tmp/ingest-s3.zip
COPY --chown=elasticsearch:root --chmod=755 docker/entrypoint.sh /usr/local/bin/s3-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/s3-entrypoint.sh"]
CMD ["eswrapper"]
