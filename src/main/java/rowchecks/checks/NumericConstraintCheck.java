package rowchecks.checks;

import java.io.Serializable;

import org.apache.spark.sql.Row;

import rowchecks.api.IRowCheck;
import utils.CheckResult;

/**
 * This class represents a <code>RowCheck</code> responsible for checking whether a column with
 * numeric values has values that are contained in an interval. The interval is defined by a min and a
 * max value, as well as whether those values (left and right respectively) are inclusive.
 * For example, if we need to test a value with minValue = 2, maxValue = 5, isLeftInclusive = true and
 * isRightInclusive = false then the check is:
 * <pre>
 *  if value is in [2, 5)
 *      value is CheckResult.PASSED
 *  else
 *      value is CheckResult.PASSED
 * </pre>
 * @param targetColumn The name of a column from our targeted Dataset that will be checked.
 * @param minValue The left / minimum value of the interval
 * @param maxValue The right / maximum value of the interval
 * @param isLeftInclusive A boolean that defines whether the value can be equal to the minimum value.
 * @param isRightInclusive A boolean that defines whether the value can be equal to the maximum value. <br>

 */
public class NumericConstraintCheck implements IRowCheck, Serializable{

    private static final long serialVersionUID = 6492963892432158520L;
	private String targetColumn;
    private double minValue = Double.NEGATIVE_INFINITY;
    private boolean isLeftInclusive = true;
    private double maxValue = Double.POSITIVE_INFINITY;
    private boolean isRightInclusive = true;

    public NumericConstraintCheck(String targetColumn, double minValue, double maxValue)
    {
        this(targetColumn, minValue, maxValue, true, true);
    }

    public NumericConstraintCheck(String targetColumn, double minValue, double maxValue, 
                                  boolean includeMinValue, boolean includeMaxValue)
    {
        this.targetColumn = targetColumn;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.isLeftInclusive = includeMinValue;
        this.isRightInclusive = includeMaxValue;
    }

    public CheckResult check(Row row) {
        double targetValue;
        try
        {
            targetValue = Double.parseDouble(row.getString(row.fieldIndex(targetColumn)));
        }
        catch (NumberFormatException e)
        {
            return CheckResult.REJECTED;
        }
        catch (IllegalArgumentException e)
        {
            return CheckResult.ILLEGAL_FIELD;
        }
        catch (NullPointerException e)
        {
            return CheckResult.MISSING_VALUE;
        }

        if ((isLeftInclusive && isRightInclusive && targetValue >= minValue && targetValue <= maxValue)
            || (!isLeftInclusive && isRightInclusive && targetValue > minValue && targetValue <= maxValue)
            || (isLeftInclusive && !isRightInclusive && targetValue >= minValue && targetValue < maxValue)
            || (!isLeftInclusive && !isRightInclusive && targetValue > minValue && targetValue < maxValue))
        { 
            return CheckResult.PASSED;
        }

        return CheckResult.REJECTED;
    }

    public String getCheckType()
    {
        String ret =  "Numeric Constraint Check On " + targetColumn + ":";
        ret += isLeftInclusive ? "[" : "(";
        ret += minValue + "," + maxValue;
        ret += isRightInclusive ? "]" : ")";
        return ret;
    }
}
