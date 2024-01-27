package utils;

/**
 * This enumerator holds the types of data that a DomainTypeCheck may check a column for.
 * The currently supported are: 
 * <ul>
 * <li> INTEGER for non-decimal number values </li>
 * <li> BOOLEAN for binary values. Support values are 1, 0, true, false, yes, no</li>
 * <li> NUMERIC for numbers both integer and decimal</li>
 * <li> ALPHA for alphanumeric values that contain at least one letter from a to z.</li>
 * </ul>
 */
public enum DomainType
    {
        INTEGER,
        BOOLEAN,
        NUMERIC,
        ALPHA
    }
