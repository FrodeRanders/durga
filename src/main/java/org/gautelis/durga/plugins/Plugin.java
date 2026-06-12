package org.gautelis.durga.plugins;

/**
 * Contract for data pipeline plugins.
 * <p>
 * Plugins receive a JSON payload and a configuration string, and return a
 * transformed JSON payload. If the transformation fails, they throw.
 */
public interface Plugin {

    /**
     * Execute the plugin against a JSON payload.
     *
     * @param payload JSON input string
     * @param config  plugin configuration string (format varies by plugin)
     * @return JSON output string
     * @throws Exception if processing fails
     */
    String execute(String payload, String config) throws Exception;
}
