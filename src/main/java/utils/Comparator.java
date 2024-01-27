package utils;

/**
 * Helper class that assists with definition of a comparing symbol. Additionally helps with using
 * comparators to compare values.
 */
public class Comparator {
    public final static String EQUAL = "==";
    public final static String NOT_EQUAL = "!=";

    public final static String GREATER = ">";
    public final static String GREATER_EQUAL = ">=";

    public final static String LESS = "<";
    public final static String LESS_EQUAL = "<=";


    public static boolean compareValues(double value1, String comparator, double value2) throws IllegalArgumentException
    {
        switch(comparator)
        {
            case EQUAL:
                return value1 == value2;
            case NOT_EQUAL:
                return value1 != value2;
            case GREATER:
                return value1 > value2;
            case GREATER_EQUAL:
                return value1 >= value2;
            case LESS:
                return value1 < value2;
            case LESS_EQUAL:
                return value1 <= value2;
            default:
                throw new IllegalArgumentException("Invalid Comparator use of " + comparator + " comparator. Only valid" +
                "comparators are: ==, !=, >, >=, <, <=");
        }
    }
}
