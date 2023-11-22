package com.rowchecks;


import java.util.HashSet;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class HashForeignKeyCheck implements IRowCheck{

    HashSet<String> keys;
    String targetColumn;
    int count = 0;
    
    public HashForeignKeyCheck(String targetColumn, Dataset<Row> foreignKeyDataset, String foreignKeyColumn)
    {
        this.targetColumn = targetColumn;

        keys = new HashSet<String>();
        List<Row> lr = foreignKeyDataset.select(foreignKeyColumn).distinct().collectAsList();
        for (Row r : lr)
        {
            count += 1;
            if (count % 125221 == 0) System.out.println(count / 125221 + "/10 A");
            keys.add(r.getString(0));
        }
        count = 0;
    }

    public CheckResult check(Row row) {
        count += 1;
        if (count % 1136073 == 0) System.out.println(count / 1136073 + "/100 B");
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

    public String getCheckType()
    {
        return "Foreign Key Restriction for column: " + targetColumn;
    }
    

    
}
