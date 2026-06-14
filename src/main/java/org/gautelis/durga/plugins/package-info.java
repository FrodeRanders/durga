/**
 * Data pipeline plugin implementations.
 * <p>
 * Plugins override either {@link Plugin#execute(String, String)} for text/JSON
 * payloads or {@link Plugin#execute(byte[], String)} for binary payloads, and are
 * registered
 * in {@code plugins/catalog.yml} with individual YAML descriptors.
 *
 * <h3>Categories</h3>
 * <ul>
 * <li><b>transform</b> — JsonTransform, FieldFilter, JsonFlatten, TypeCoercion, StringTemplate, RegexExtract</li>
 * <li><b>validate</b> — JsonSchemaValidator</li>
 * <li><b>enrich</b> — KvEnricher, UuidInject, PiiMask, TimestampNormalize</li>
 * <li><b>route</b> — DeadLetterRouter</li>
 * <li><b>aggregate</b> — WindowCounter</li>
 * </ul>
 *
 * @see Plugin
 */
package org.gautelis.durga.plugins;
