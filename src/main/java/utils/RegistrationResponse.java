package utils;

/**
 * This enumerator holds the possible responses when registering a new <code>DatasetProfile</code>.
 * SUCCESS is returned when the profile is registered, FAILURE when something went wrong and ALIAS_EXISTS
 * when the given alias already belongs to another profile.
 */
public enum RegistrationResponse {
    SUCCESS,
    FAILURE,
    ALIAS_EXISTS
}
