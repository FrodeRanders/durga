/**
 * Data pipeline plugin implementations.
 * <p>
 * Plugins implement {@link Plugin#execute(byte[], String)} and are registered
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
