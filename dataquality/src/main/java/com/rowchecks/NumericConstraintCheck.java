package com.rowchecks;

import org.apache.spark.sql.Row;

public class NumericConstraintCheck implements IRowCheck{

    String targetColumn;
    double minValue = Double.NEGATIVE_INFINITY;
    boolean isLeftInclusive = true;
    double maxValue = Double.POSITIVE_INFINITY;
    boolean isRightInclusive = true;

    public NumericConstraintCheck(String targetColumn, double minValue, double maxValue)
    {
        this.targetColumn = targetColumn;
        this.minValue = minValue;
        this.maxValue = maxValue;
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
            return CheckResult.FAILED;
        }
        catch (IllegalArgumentException e)
        {
            return CheckResult.ILLEGAL_FIELD;
        }
        catch (NullPointerException e)
        {
            return CheckResult.MISSING_VALUE;
        }

        if (isLeftInclusive && isRightInclusive && targetValue >= minValue && targetValue <= maxValue) return CheckResult.PASSED;
        if (!isLeftInclusive && isRightInclusive && targetValue > minValue && targetValue <= maxValue) return CheckResult.PASSED;
        if (isLeftInclusive && !isRightInclusive && targetValue >= minValue && targetValue < maxValue) return CheckResult.PASSED;
        if (!isLeftInclusive && !isRightInclusive && targetValue > minValue && targetValue < maxValue) return CheckResult.PASSED;
        return CheckResult.FAILED;
    }


    public String getCheckType() {
        return "Numeric Constraint Check: " + getConstraint() ;
    }

    public String getConstraint()
    {
        String ret = isLeftInclusive ? "[" : "(";
        ret += minValue + ", " + maxValue;
        ret += isRightInclusive ? "]" : ")";
        return ret + " for column: " + targetColumn;
    }
    
}
