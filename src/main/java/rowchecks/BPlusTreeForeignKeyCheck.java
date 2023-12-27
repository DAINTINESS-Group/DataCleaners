package rowchecks;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import utils.BPlusTreeWrapper;
import utils.CheckResult;



public class BPlusTreeForeignKeyCheck implements IRowCheck, Serializable {
 
    private static int globalIdCounter = 0;

    private String targetColumn;
    private String foreignKeyColumn;
    private BPlusTreeWrapper bTreeWrapper;

    public BPlusTreeForeignKeyCheck(String targetColumn, Dataset<Row> foreignKeyDataset, String foreignKeyColumn)
    {
        this.targetColumn = targetColumn;
        this.foreignKeyColumn = foreignKeyColumn;
        bTreeWrapper = new BPlusTreeWrapper("FKDB" + globalIdCounter);
        globalIdCounter++;

        try
        { 
            foreignKeyDataset.foreach(row -> {insertForeignKey(row);});
        }
        catch (Exception e)
        {
            System.out.println("FOREIGN KEY BTREE ERROR:" + e);
        }
    }

    public CheckResult check(Row row)
    {
        if (bTreeWrapper.getBPlusTree() == null) { bTreeWrapper.initDatabase(false); }

        String targetValue = row.getString(row.fieldIndex(targetColumn));
        try
        {
            if (bTreeWrapper.getBPlusTree().get(null, new DatabaseEntry(targetValue.getBytes()),
                 new DatabaseEntry("".getBytes()), LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
            {
                return CheckResult.PASSED;
            }
        }
        catch (DatabaseException e)
        {
            System.err.println(e);
            return CheckResult.INTERNAL_ERROR;
        }
        catch (NullPointerException e)
        {
            return CheckResult.MISSING_VALUE;
        }
        return CheckResult.REJECTED;
    }

    private void insertForeignKey(Row row)
    {
        
        try
        {
            if (bTreeWrapper.getBPlusTree() == null) { bTreeWrapper.initDatabase(true); }

            String targetValue = row.getString(row.fieldIndex(foreignKeyColumn));
            bTreeWrapper.getBPlusTree().put(null, new DatabaseEntry(targetValue.getBytes()), new DatabaseEntry("".getBytes()));
        }
        catch (NullPointerException e) //Null values are ignored.
        {
            return;
        }
        catch (DatabaseException e)
        {
            //TODO: Handle Database exceptions somehow.
            System.out.println(e);
            return;
        }
    }

    public String getCheckType()
    {
        return "";
    }
}
