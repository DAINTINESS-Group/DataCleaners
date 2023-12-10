package rowchecks;

import org.apache.spark.sql.Row;

import model.rowcheckresults.RowResultFactory;
import utils.CheckResult;
import utils.VioletingRowPolicy;

public class NotNullCheck extends GenericRowCheck {

    private String targetColumn;

    public NotNullCheck(String targetColumn)
    {
        this.targetColumn = targetColumn;
        checkResult = new RowResultFactory().createNotNullCheckResult(targetColumn);
    }

    public CheckResult check(Row row, VioletingRowPolicy violetingRowPolicy) {
        try
        {
            if (row.isNullAt(row.fieldIndex(targetColumn)))
            {
                addRejectedRow(row, violetingRowPolicy);
                return CheckResult.FAILED;
            }
            addApprovedRow(row, violetingRowPolicy);
            return CheckResult.PASSED;
        }
        catch (IllegalArgumentException e)
        {
            addInvalidRow(row, violetingRowPolicy);
            return CheckResult.ILLEGAL_FIELD;
        }
    }
    
}
