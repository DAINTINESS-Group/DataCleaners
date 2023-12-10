package model;

public class ClientRequestResponse {
    
    private long numberOfInvalidRows;
    private long numberOfRejectedRows;
    private boolean isSuccesful;

    public ClientRequestResponse(boolean isSuccesful, long rejectedRows, long invalidRows)
    {
        this.isSuccesful = isSuccesful;
        numberOfRejectedRows = rejectedRows;
        numberOfInvalidRows = invalidRows;
    }

    public long getNumberOfInvalidRows() { return numberOfInvalidRows; }
    public long getNumberOfRejectedRows() { return numberOfRejectedRows; }
    public boolean isSuccesful() { return isSuccesful; }
}
