package rowchecks;


import java.io.Serializable;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import utils.CheckResult;

public class HashForeignKeyCheck implements IRowCheck, Serializable{

    private HashSet<String> keys;
    private String targetColumn;
    private String foreinKeyColumn;

    public HashForeignKeyCheck(String targetColumn, Dataset<Row> foreignKeyDataset, String foreignKeyColumn)
    {
        this.targetColumn = targetColumn;
        this.foreinKeyColumn = foreignKeyColumn;

        keys = new HashSet<String>();
        List<Row> lr = foreignKeyDataset.select(foreignKeyColumn).distinct().collectAsList();
        for (Row r : lr)
        {
            keys.add(r.getString(0));
        }
    }

    public CheckResult check(Row row) {
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
        return CheckResult.REJECTED;
    }

    public String getCheckType()
    {
        return "Foreign Check On " + targetColumn + "->" + foreinKeyColumn;
    }
    
}
