package model;

import java.io.Serializable;
import java.util.ArrayList;

import model.rowcheckresults.IRowCheckResult;

public class ServerRequestResult implements Serializable{
    
    private ArrayList<IRowCheckResult> rowCheckResults = new ArrayList<IRowCheckResult>();
    private long rejectedRows = 0;
    private long invalidRows = 0;


    public ArrayList<IRowCheckResult> getRowCheckResults() {
        return rowCheckResults;
    }

    public void addRowCheckResult(IRowCheckResult result) { rowCheckResults.add(result); }

    public void increaseRejectedRows() { rejectedRows++; }
    public void increaseInvalidRows() { invalidRows++; }
    public void setRejectedRows(long i) { rejectedRows = i; }
    public void setInvalidRows(long i) { invalidRows = i; }
    public long getRejectedRows() {
        return rejectedRows;
    }

    public long getInvalidRows() {
        return invalidRows;
    }




    
}
