package rowchecks;

import org.apache.spark.sql.Row;

import model.rowcheckresults.RowResultFactory;
import utils.CheckResult;
import utils.VioletingRowPolicy;

public class NumericConstraintCheck extends GenericRowCheck{

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
        this.checkResult = new RowResultFactory().createNumericConstraintCheckResult(targetColumn, minValue,
                                                                maxValue, isLeftInclusive, isRightInclusive);
    }

    public CheckResult check(Row row, VioletingRowPolicy violetingRowPolicy) {
        double targetValue;
        try
        {
            targetValue = Double.parseDouble(row.getString(row.fieldIndex(targetColumn)));
        }
        catch (NumberFormatException e)
        {
            addInvalidRow(row, violetingRowPolicy);
            return CheckResult.FAILED;
        }
        catch (IllegalArgumentException e)
        {
            addInvalidRow(row, violetingRowPolicy);
            return CheckResult.ILLEGAL_FIELD;
        }
        catch (NullPointerException e)
        {
            addInvalidRow(row, violetingRowPolicy);
            return CheckResult.MISSING_VALUE;
        }

        if ((isLeftInclusive && isRightInclusive && targetValue >= minValue && targetValue <= maxValue)
            || (!isLeftInclusive && isRightInclusive && targetValue > minValue && targetValue <= maxValue)
            || (isLeftInclusive && !isRightInclusive && targetValue >= minValue && targetValue < maxValue)
            || (!isLeftInclusive && !isRightInclusive && targetValue > minValue && targetValue < maxValue))
        { 
            addApprovedRow(row, violetingRowPolicy);
            return CheckResult.PASSED;
        }

        addRejectedRow(row, violetingRowPolicy);
        return CheckResult.FAILED;
    }

}
