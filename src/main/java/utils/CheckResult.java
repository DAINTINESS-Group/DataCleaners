package utils;

/**
 * This enumerator holds the values an entry can have as a result after any <code>RowCheck</code>. The possible results
 * supported are:
 * <ul>
 * <li> PASSED: Given to entries that pass the check succesfully. </li>
 * <li> REJECTED: Given to entries that failed the check. Contributes to rejected entries. </li>
 * <li> MISSING_VALUE: Given to entries that have unexpected null values. Contributes to invalid entries. </li>
 * <li> ILLEGAL_FIELD: Given to entries that contain variables/columns that don't exist or the check has expressions
 * with illegal characters. Contributes to invalid entries.</li>
 * <li> INTERNAL_ERROR: Given to entries when an error occurs (or when an afforementioned result is not given). Contributes
 * to invalid entries. </li>
 * </ul>
 */
public enum CheckResult
{
    PASSED,
    REJECTED,
    MISSING_VALUE,
    ILLEGAL_FIELD,
    INTERNAL_ERROR
}
