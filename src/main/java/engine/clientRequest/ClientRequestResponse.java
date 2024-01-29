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
 * @param errorDetails A string that defines, if an error has occured, what the error is. Is null if isSuccesful is true.
 * Additionally, if a result does not reach the execution stage the <code>numberOfRejectedRows</code> and <code>numberOfInvalidRows</code>
 * will be equal to -1, with the <code>errorDetails</code> explaining what went wrong.
 * 
 * @see utils.CheckResult
 * @see engine.ServerToClientResponseTranslator
 */
public class ClientRequestResponse {
    
    private long numberOfInvalidRows;
    private long numberOfRejectedRows;
    private boolean isSuccesful;
    private String errorDetails;

    public ClientRequestResponse(boolean isSuccesful, long rejectedRows, long invalidRows)
    {
        this.isSuccesful = isSuccesful;
        numberOfRejectedRows = rejectedRows;
        numberOfInvalidRows = invalidRows;

        if (numberOfInvalidRows > 0) { this.errorDetails = "Invalid entries detected."; }
    }

    public ClientRequestResponse(String errorDetails)
    {
        this.isSuccesful = false;
        numberOfInvalidRows = -1;
        numberOfRejectedRows = -1;
        this.errorDetails = errorDetails;
    }

    public long getNumberOfInvalidRows() { return numberOfInvalidRows; }
    public long getNumberOfRejectedRows() { return numberOfRejectedRows; }
    public boolean isSuccesful() { return isSuccesful; }
    public String getErrorDetails() { return errorDetails; }
}
