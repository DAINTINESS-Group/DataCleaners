package rowchecks;

import java.io.Serializable;

import org.apache.spark.sql.Row;

import utils.CheckResult;

public class NotNullCheck implements IRowCheck, Serializable {

    private String targetColumn;

    public NotNullCheck(String targetColumn)
    {
        this.targetColumn = targetColumn;
    }

    public CheckResult check(Row row) {
        try
        {
            if (row.isNullAt(row.fieldIndex(targetColumn)))
            {
                return CheckResult.FAILED;
            }
            return CheckResult.PASSED;
        }
        catch (IllegalArgumentException e)
        {
            return CheckResult.ILLEGAL_FIELD;
        }
    }
    
    public String getCheckType()
    {
        return "Null Check On " + targetColumn;
    }
}
