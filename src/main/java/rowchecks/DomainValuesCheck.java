package rowchecks;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import utils.CheckResult;

import org.apache.spark.sql.Row;

public class DomainValuesCheck implements IRowCheck, Serializable {

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
        return CheckResult.FAILED;
    }

    public String getCheckType()
    {
        return "Domain Values On " + targetColumn;
    }
}
