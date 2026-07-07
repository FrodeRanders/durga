package org.gautelis.durga.plugins;

/**
 * Execution context passed to a plugin so it can adapt to the mode the generated
 * worker is running in.
 * <p>
 * The primary distinction is {@link #validationMode()}: in validation mode a plugin
 * must not perform substantial side effects (external writes, mutations). It should
 * instead compute and return the response it <em>would</em> have produced, so a
 * candidate process can be compared against the running one without modifying
 * anything outside the validation topics.
 * <p>
 * The type is intentionally a small, forward-compatible object rather than a bare
 * boolean, so additional per-execution context (e.g. candidate version, tracing)
 * can be added without changing the {@link Plugin} signature.
 */
public final class PluginExecutionContext {

    private static final PluginExecutionContext PRODUCTION = new PluginExecutionContext(false);
    private static final PluginExecutionContext VALIDATION = new PluginExecutionContext(true);

    private final boolean validationMode;

    private PluginExecutionContext(boolean validationMode) {
        this.validationMode = validationMode;
    }

    /** Context for a normal production execution: side effects are performed. */
    public static PluginExecutionContext production() {
        return PRODUCTION;
    }

    /** Context for a validation-mode execution: substantial side effects must be suppressed. */
    public static PluginExecutionContext validation() {
        return VALIDATION;
    }

    /**
     * @return {@code true} when the plugin runs as part of a validation-mode shadow process and
     *         must not perform substantial side effects.
     */
    public boolean validationMode() {
        return validationMode;
    }
}
