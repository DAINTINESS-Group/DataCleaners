package engine.clientRequest;
/**
 * This class represents a completed ClientReqest, that has been executed and returned to the client. It holds
 * basic information on the executed request's result.
 * @param isSuccesful A boolean value that determines whether the request was executed without issues. Essentially
 * is true is the number of invalid rows is 0.
 * @param numberOfRejectedRows A long value that represents the number of rows that received the <code>CheckResult.REJECTED</code>
 * result.
 * @param numberOfInvalidRows A long value that represents the number of rows that did NOT received the <code>CheckResult.PASSED</code> or
 * <code>CheckResult.REJECTED</code> result.
 * 
 * @see utils.CheckResult
 * @see engine.ServerToClientResponseTranslator
 */
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
