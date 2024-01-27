package utils;

/**
 * This enumerator holds the format types used in a FormatCheck.
 * The currently supported types are of dates and are:
 * <ul>
 * <li> DD_MM_YYYY </li>
 * <li> MM_DD_YYYY </li>
 * <li> YYYY_MM_DD </li>
 * <li> DD_MM </li>
 * <li> MM_DD </li>
 * <li> YYYY_MM </li>
 * <li> MM_YYYY </li>
 * </ul>
 * where DD represents the days, MM represents the month and YYYY represents the year.
 */
public enum FormatType {
    DD_MM_YYYY,
    MM_DD_YYYY,
    YYYY_MM_DD,
    DD_MM,
    MM_DD,
    YYYY_MM,
    MM_YYYY
}
