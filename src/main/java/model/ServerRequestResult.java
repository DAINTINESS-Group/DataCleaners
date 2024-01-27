package model;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

/**
 * This class represents the result of an executed <code>ServerRequest</code>. Containted inside a <code>ServerRequest</code>,
 * it contains detailed information on the outcome of each check for each entry.
 * 
 * @param rowCheckTypes An array of Strings that contain a brief description of each row check. Used in report generation
 * and matches one-to-one with the columns in <code>rowCheckResults</code>
 * @param rejectedRows A long that contains the number of rejected rows.
 * @param invalidRows A long that contains the number of invalid rows (aka rows that were neither rejected nor passed)
 * @param rowCheckResults A <code>Dataset</code> with columns equal to the number of <code>RowChecks</code> and rows equal
 * to the number of entries tested under this request. Each entry has the <code>CheckResult</code> value given to it by the
 * corresponding check, in string format:
 * 
 * <table border='1'>
 * <tr><th>_id</th><th>c1</th><th>c2</th><th>c3</th></tr>
 * <tr><td>1</td><td>PASSED</td><td>PASSED</td><td>PASSED</td></tr>
 * <tr><td>2</td><td>PASSED</td><td>REJECTED</td><td>PASSED</td></tr>
 * <tr><td>3</td><td>ILLEGAL_FIELD</td><td>MISSING_VALUE</td><td>REJECTED</td></tr>
 * </table>
 *  */
public class ServerRequestResult implements Serializable{
    
    private static final long serialVersionUID = -646765944375157319L;
	private Dataset<Row> rowCheckResults;
    private ArrayList<String> rowCheckTypes;
    private long rejectedRows = 0;
    private long invalidRows = 0;


    public void applyRowCheckResults(Dataset<Row> rowCheckResults, ArrayList<String> rowCheckTypes) 
    {
        //Persist data due to lazy evaluation and BPlusTree Interaction.
        rowCheckResults = rowCheckResults.persist(StorageLevel.MEMORY_AND_DISK());
        rejectedRows = rowCheckResults.where("value LIKE '%REJECTED%'").count();
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
