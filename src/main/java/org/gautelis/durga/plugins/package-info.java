/**
 * Data pipeline plugin implementations.
 * <p>
 * Plugins implement {@link Plugin#execute(String, String)} and are registered
 * in {@code plugins/catalog.yml} with individual YAML descriptors.
 *
 * <h3>Category contracts</h3>
 * <ul>
 * <li><b>transform</b> — JsonTransform, FieldFilter</li>
 * <li><b>enrich</b> — KvEnricher</li>
 * <li><b>route</b> — DeadLetterRouter</li>
 * <li><b>aggregate</b> — WindowCounter</li>
 * </ul>
 *
 * @see Plugin
 */
package org.gautelis.durga.plugins;
