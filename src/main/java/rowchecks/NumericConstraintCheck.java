package rowchecks;

import java.io.Serializable;

import org.apache.spark.sql.Row;

import utils.CheckResult;

public class NumericConstraintCheck implements IRowCheck, Serializable{

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
