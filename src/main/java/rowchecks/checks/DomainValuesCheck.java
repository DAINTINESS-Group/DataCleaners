package rowchecks.checks;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.sql.Row;

import rowchecks.api.IRowCheck;
import utils.CheckResult;

public class DomainValuesCheck implements IRowCheck, Serializable {

    private static final long serialVersionUID = 3737639305672122256L;
	private ArrayList<String> domainValues;
    private String targetColumn;

    public DomainValuesCheck(String targetColumn, String[] domainValues)
    {
        this.domainValues = new ArrayList<String>(Arrays.asList(domainValues));
        this.targetColumn = targetColumn;
    }

    public CheckResult check(Row row) 
    {
        try
        {
            if (domainValues.contains(row.getString(row.fieldIndex(targetColumn))))
            {
                return CheckResult.PASSED;
            } 
        }
        catch (IllegalArgumentException e)
        {
            return CheckResult.ILLEGAL_FIELD;
        }
        catch (NullPointerException e)
        {
            return CheckResult.MISSING_VALUE;
        }
        return CheckResult.REJECTED;
    }

    public String getCheckType()
    {
        return "Domain Values On " + targetColumn;
    }
}
