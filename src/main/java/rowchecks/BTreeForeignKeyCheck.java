package rowchecks;

import java.io.File;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import btree4j.BTree;
import btree4j.BTreeException;
import btree4j.Value;
import btree4j.utils.io.FileUtils;
import utils.CheckResult;



public class BTreeForeignKeyCheck implements IRowCheck {
 
    private static class ForEachInserter implements ForeachFunction<Row>
    {
        static BTree btree;

        public ForEachInserter(BTree tree)
        {
            ForEachInserter.btree = tree;
        }

        public void call(Row row) throws Exception {
            String val = row.getString(0);
            btree.addValue(new Value(val), 1);
        }
    }


    private String targetColumn;
    private String foreignKeyColumn;
    
    static BTree btree; //TODO: Can't make BTree serializable. Find sollution. ALSO CHANGE TO BPLUSTREE

    public BTreeForeignKeyCheck(String targetColumn, Dataset<Row> foreignKeyDataset, String foreignKeyColumn)
    {
        this.targetColumn = targetColumn;
        this.foreignKeyColumn = foreignKeyColumn;
        try
        { 
            File tempDir = FileUtils.getTempDir();
            File tmpFile = new File(tempDir, "BTreeStorage.idx");
            
            tmpFile.deleteOnExit();
            if (tmpFile.exists()) tmpFile.delete();
   
            btree = new BTree(tmpFile);
            btree.init(true);
            btree.setBulkloading(true, 1);
            foreignKeyDataset.select(foreignKeyColumn).distinct().foreach(new ForEachInserter(btree));
        }
        catch (Exception e)
        {
            System.out.println("FOREIGN KEY BTREE ERROR:" + e);
        }
    }

    public CheckResult check(Row row)
    {
        String targetValue = row.getString(row.fieldIndex(targetColumn));
        try
        {
            if (btree.findValue(new Value(targetValue)) == 1)
            {
                return CheckResult.PASSED;
            }
        }
        catch (BTreeException e)
        {
            System.err.println(e);
            return CheckResult.INTERNAL_ERROR;
        }
        catch (NullPointerException e)
        {
            return CheckResult.FAILED; //TODO: If value is NULL, do we fail it? 
        }
        return CheckResult.FAILED;
    }

    public String getCheckType()
    {
        return "";
    }
}
