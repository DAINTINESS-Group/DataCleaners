package utils;

/**
 * This enumerator holds the three available tactics on how to handle rejected rows. The available
 * tactics are:
 * <ul>
 * <li> WARN: A log is generated with the rejected/invalid rows and nothing more.</li>
 * <li> PURGE: A log is generated with the rejected/invalid rows and a CSV with the passed entries.</li>
 * <li> ISOLATE: A log is generated with the rejected/invalid rows and two CSVs, one with passed and one with failed entries.</li>
 * </ul>
 */
public enum ViolatingRowPolicy
    {
        ISOLATE,
        PURGE,
        WARN
    }
