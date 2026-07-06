/**
 * Validation-mode contracts for shadow-running candidate task implementations against production.
 * <p>
 * A candidate (not-yet-released) task implementation can be run against real input on a dedicated
 * consumer index with all side effects suppressed, its output diverted rather than published, and
 * compared per task against the prior/production output for the same input. This package holds the
 * wire types exchanged between the shadow worker and the comparator ({@link
 * org.gautelis.durga.validation.ValidationCandidateOutput}, {@link
 * org.gautelis.durga.validation.ValidationResult}) and the structural comparison used to produce
 * the report ({@link org.gautelis.durga.validation.JsonComparison}).
 */
package org.gautelis.durga.validation;
