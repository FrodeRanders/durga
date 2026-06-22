package org.gautelis.durga;

import java.util.Map;

/**
 * Reference to a logical data asset produced or consumed by a pipeline activity.
 * <p>
 * Large pipeline data should normally travel by reference. Kafka process events can carry this
 * handle in their payload while bytes live in S3, PostgreSQL, Neo4j, local files, or another
 * data store.
 *
 * @param name logical BPMN data object name
 * @param uri physical location or stable lookup URI
 * @param mediaType content type, for example {@code application/json} or {@code application/parquet}
 * @param schema schema identifier, class name, or schema resource path
 * @param metadata operational metadata such as row count, checksum, partition, or store kind
 */
public record DataHandle(
        String name,
        String uri,
        String mediaType,
        String schema,
        Map<String, Object> metadata
) {
    public DataHandle {
        metadata = metadata != null ? Map.copyOf(metadata) : Map.of();
    }
}
