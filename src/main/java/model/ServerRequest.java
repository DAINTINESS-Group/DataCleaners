package model;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import model.rowcheckresults.IRowCheckResult;
import rowchecks.IRowCheck;
import utils.CheckResult;
import utils.VioletingRowPolicy;

public class ServerRequest implements Serializable{
    
    //TODO: Add Hollistic and Group Checks.
    private DatasetProfile targetProfile;
    private VioletingRowPolicy violetingRowPolicy;

    private ArrayList<IRowCheck> rowChecks;
    private ServerRequestResult requestResult;

    //TODO: Archive variables (static) required to pass info from within ForEach function to class. Somehow remove these.
    private static ArrayList<IRowCheckResult> rowCheckResultArchive = new ArrayList<IRowCheckResult>();
    private static ArrayList<Boolean> rowCheckValidityArchive = new ArrayList<Boolean>();
    private static long datasetSize;
    private static ServerRequestResult resultArchive = null;

    public ServerRequest(VioletingRowPolicy violetingRowPolicy)
    {
        this.violetingRowPolicy = violetingRowPolicy;
        rowChecks = new ArrayList<IRowCheck>();
    }

    public ServerRequestResult execute()
    {
        requestResult = new ServerRequestResult();

        Dataset<Row> dataset = targetProfile.getDataset();

        datasetSize = dataset.count();

        dataset.foreach(row -> { executeRowChecks(row); });

        for (int i = 0; i < rowChecks.size(); i++)
        {
            IRowCheckResult res = rowCheckResultArchive.get(i);
            res.setValidity(rowCheckValidityArchive.get(i));

            rowChecks.get(i).setCheckResult(res);
            requestResult.addRowCheckResult(res);
        }
        requestResult.setRejectedRows(resultArchive.getRejectedRows());
        requestResult.setInvalidRows(resultArchive.getInvalidRows());

        return requestResult;
    }

    private void executeRowChecks(Row row)
    { 
        for (IRowCheck rowCheck: rowChecks)
        {
            CheckResult result = rowCheck.check(row, violetingRowPolicy);

            if (result == CheckResult.PASSED)
            {
                continue;
            }
            else if (result == CheckResult.FAILED)
            {
                requestResult.increaseRejectedRows();
            }
            else
            {
                requestResult.increaseInvalidRows();
            }
        }

        datasetSize--;
        if (datasetSize == 0)
        {
            rowCheckResultArchive.clear();
            rowCheckValidityArchive.clear();
            for (int i = 0; i < rowChecks.size(); i++)
            {
                rowCheckResultArchive.add(rowChecks.get(i).getCheckResult());
                rowCheckValidityArchive.add(rowChecks.get(i).getCheckResult().isSuccesful());
            }
            resultArchive = requestResult;
        }
    }

    public void addRowCheck(IRowCheck check) { rowChecks.add(check); }
    public void setProfile(DatasetProfile profile) { this.targetProfile = profile; }

    public ArrayList<IRowCheck> getRowChecks() { return rowChecks; }
    public DatasetProfile getProfile() { return targetProfile; }
    public ServerRequestResult getRequestResult() { return requestResult; }
    public VioletingRowPolicy getVioletingRowPolicy() { return violetingRowPolicy; }

    public void setRowChecks(ArrayList<IRowCheck> checks) { rowChecks = checks; }
}
