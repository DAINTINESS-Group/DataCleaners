package rowchecks.checks;

import java.io.Serializable;

import org.apache.spark.sql.Row;

import rowchecks.api.IRowCheck;
import utils.CheckResult;

public class NotNullCheck implements IRowCheck, Serializable {

    private static final long serialVersionUID = 2396254117364726147L;
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
                return CheckResult.REJECTED;
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