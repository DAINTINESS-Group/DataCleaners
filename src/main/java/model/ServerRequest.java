package model;

import java.io.Serializable;
import java.util.ArrayList;

import rowchecks.api.IRowCheck;
import utils.ViolatingRowPolicy;

/**
 * This class represents a translated <code>ClientRequest</code> prepared for execution. If executed, it also contains
 * a <code>ServerRequestResult</code> that contains information on rejected/passed entries of the request's <code>Dataset</code>.
 * @param targetProfile The <code>DatasetProfile</code> this request belongs to.
 * @param violatingRowPolicy The <code>ViolatingRowPolicy</code> this request follows.
 * @param rowChecks An array of all <code>RowChecks</code> that are checked during execution.
 * @param requestResult The result of this request's execution. Can be null if the execution has not been completed.
 * 
 * @see ViolatingRowPolicy
 * @see IRowCheck
 * @see DatasetProfile
 * @see ServerRequestResult
 */
public class ServerRequest implements Serializable{
    
    private static final long serialVersionUID = 5849658787172344510L;

    private DatasetProfile targetProfile;
    private ViolatingRowPolicy violatingRowPolicy;

    private ArrayList<IRowCheck> rowChecks;
    private ServerRequestResult requestResult;

    public ServerRequest(ViolatingRowPolicy violatingRowPolicy)
    {
        this.violatingRowPolicy = violatingRowPolicy;
        rowChecks = new ArrayList<IRowCheck>();
    }


    public void addRowCheck(IRowCheck check) { rowChecks.add(check); }
    public void setProfile(DatasetProfile profile) { this.targetProfile = profile; }

    public ArrayList<IRowCheck> getRowChecks() { return rowChecks; }
    public DatasetProfile getProfile() { return targetProfile; }
    public ServerRequestResult getRequestResult() { return requestResult; }
    public ViolatingRowPolicy getViolatingRowPolicy() { return violatingRowPolicy; }

    public void setRowChecks(ArrayList<IRowCheck> checks) { rowChecks = checks; }
    public void setRequestResult(ServerRequestResult res) { requestResult = res; }
}
