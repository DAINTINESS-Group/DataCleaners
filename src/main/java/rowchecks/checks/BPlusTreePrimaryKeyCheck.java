package rowchecks.checks;

import java.io.Serializable;

import org.apache.spark.sql.Row;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;

import rowchecks.api.IRowCheck;
import utils.BPlusTreeWrapper;
import utils.CheckResult;

public class BPlusTreePrimaryKeyCheck implements IRowCheck, Serializable {

	private static final long serialVersionUID = 4887311250208642538L;

	private static int globalIdCounter = 0;

    private String targetColumn;
    private String dbName;

    private BPlusTreeWrapper bTreeWrapper;

    public BPlusTreePrimaryKeyCheck(String targetColumn)
    {
        this.targetColumn = targetColumn;
        this.dbName = "PKDB" + globalIdCounter;
        globalIdCounter++;

        bTreeWrapper = new BPlusTreeWrapper(dbName);
    }

    public CheckResult check(Row row) 
    {
        if (bTreeWrapper.getBPlusTree() == null) { bTreeWrapper.initDatabase(true); }
        try
        {
            String targetValue = row.getString(row.fieldIndex(targetColumn));
            
            OperationStatus status = bTreeWrapper.getBPlusTree()
                                        .putNoDupData(null, new DatabaseEntry(targetValue.getBytes()), new DatabaseEntry("".getBytes()));

            if (status == OperationStatus.SUCCESS) { return CheckResult.PASSED; }
            else if (status == OperationStatus.KEYEXIST) { return CheckResult.REJECTED; }
        }
        catch (NullPointerException e)
        {
            return CheckResult.MISSING_VALUE;
        }
        return CheckResult.INTERNAL_ERROR;
    }

    
    public String getCheckType() {
        return "Primary key check on: " + targetColumn;
    }
}
