package rowchecks;

import org.apache.spark.sql.Row;

public class NotNullCheck implements IRowCheck {

    private String targetColumn;

    public NotNullCheck(String targetColumn)
    {
        this.targetColumn = targetColumn;
    }

    public CheckResult check(Row row) {
        try
        {
            return row.isNullAt(row.fieldIndex(targetColumn)) ? CheckResult.FAILED : CheckResult.PASSED;
        }
        catch (IllegalArgumentException e)
        {
            return CheckResult.ILLEGAL_FIELD;
        }
    }

    @Override
    public String getCheckType() {
        return "Null Value Check on column: " + targetColumn;
    }
    
}
