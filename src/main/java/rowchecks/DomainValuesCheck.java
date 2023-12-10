package rowchecks;

import java.util.ArrayList;
import java.util.Arrays;
import utils.CheckResult;
import utils.VioletingRowPolicy;

import org.apache.spark.sql.Row;

import model.rowcheckresults.RowResultFactory;

public class DomainValuesCheck extends GenericRowCheck {

    private ArrayList<String> domainValues;
    private String targetColumn;

    public DomainValuesCheck(String targetColumn, String[] domainValues)
    {
        this.domainValues = new ArrayList<String>(Arrays.asList(domainValues));
        this.targetColumn = targetColumn;
        checkResult = new RowResultFactory().createDomainValuesCheckResult(targetColumn);
    }

    public CheckResult check(Row row, VioletingRowPolicy violetingRowPolicy) 
    {
        try
        {
            if (domainValues.contains(row.getString(row.fieldIndex(targetColumn))))
            {
                addApprovedRow(row, violetingRowPolicy);
                return CheckResult.PASSED;
            } 
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
        addRejectedRow(row, violetingRowPolicy);
        return CheckResult.FAILED;
    }
}
