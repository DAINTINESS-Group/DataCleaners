package rowchecks.checks;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import rowchecks.api.IRowCheck;
import utils.BPlusTreeWrapper;
import utils.CheckResult;


/**
 * This class represents a <code>RowCheck</code> responsible for checking a Foreign Key relationship between
 * two columns.
 * @param targetColumn The name of a column from our targeted Dataset
 * @param foreignKeyDataset The dataset from which we extract another column to test for foreign key relationship. This can
 * be the same as the targeted Dataset.
 * @param foreignKeyColumn The name of a column from the foreignKeyDataset, which will be
 * the existing values from which we test the foreign key relationship with. These values are stored in a disk-based BPlusTree
 * data structure, and thus rely heavily on the disk capacity.
 */
public class BPlusTreeForeignKeyCheck implements IRowCheck, Serializable {
 
    private static final long serialVersionUID = 1915838995252217241L;

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
        catch (DatabaseException e)
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

    private void insertForeignKey(Row row) throws DatabaseException
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

    }

    public String getCheckType()
    {
        return "Foreign Key Check " + targetColumn + " -> " + foreignKeyColumn;
    }
}
