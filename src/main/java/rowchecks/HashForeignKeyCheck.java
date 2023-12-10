package rowchecks;


import java.util.HashSet;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import model.rowcheckresults.IRowCheckResult;
import utils.CheckResult;
import utils.VioletingRowPolicy;

public class HashForeignKeyCheck implements IRowCheck{

    private HashSet<String> keys;
    private String targetColumn;
    
    private IRowCheckResult checkResult;
    
    public HashForeignKeyCheck(String targetColumn, Dataset<Row> foreignKeyDataset, String foreignKeyColumn)
    {
        this.targetColumn = targetColumn;

        keys = new HashSet<String>();
        List<Row> lr = foreignKeyDataset.select(foreignKeyColumn).distinct().collectAsList();
        for (Row r : lr)
        {
            keys.add(r.getString(0));
        }
    }

    public CheckResult check(Row row,  VioletingRowPolicy violetingRowPolicy) {
        try
        {
            if (keys.contains(row.getString(row.fieldIndex(targetColumn)))) return CheckResult.PASSED;
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

    public IRowCheckResult getCheckResult()
    {
        return checkResult;
    }

    public void setCheckResult(IRowCheckResult result)
    {
        checkResult = result;
    }
    
}
