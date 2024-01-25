package model;

import java.io.Serializable;
import java.util.ArrayList;

import rowchecks.api.IRowCheck;
import utils.ViolatingRowPolicy;

public class ServerRequest implements Serializable{
    
    private static final long serialVersionUID = 5849658787172344510L;
	//TODO: Add Hollistic and Group Checks.
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
