package model;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

public class ServerRequestResult implements Serializable{
    
    private Dataset<Row> rowCheckResults;
    private ArrayList<String> rowCheckTypes;
    private long rejectedRows = 0;
    private long invalidRows = 0;


    public void applyRowCheckResults(Dataset<Row> rowCheckResults, ArrayList<String> rowCheckTypes) 
    {
        //Persist data due to lazy evaluation and BPlusTree Interaction.
        rowCheckResults = rowCheckResults.persist(StorageLevel.MEMORY_AND_DISK());
        rejectedRows = rowCheckResults.where("value LIKE '%FAILED%'").count();
        invalidRows = rowCheckResults.where("value LIKE '%MISSING_VALUE%' OR value LIKE '%ILLEGAL_FIELD%' OR value LIKE '%INTERNAL_ERROR%'").count();
        this.rowCheckResults = rowCheckResults.drop("value", "values");
        this.rowCheckTypes = rowCheckTypes;
    }

    public Dataset<Row> getRowCheckResults() {
        return rowCheckResults;
    }


    public void increaseRejectedRows() { rejectedRows++; }
    public void increaseInvalidRows() { rejectedRows++; }
    
    public ArrayList<String> getRowCheckTypes() { return rowCheckTypes; }
    public long getRejectedRows() {
        return rejectedRows;
    }

    public long getInvalidRows() {
        return invalidRows;
    }




    
}
