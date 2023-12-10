package rowchecks;

import org.apache.spark.sql.Row;

import model.rowcheckresults.IRowCheckResult;
import utils.CheckResult;
import utils.VioletingRowPolicy;

public interface IRowCheck {

    public CheckResult check(Row row, VioletingRowPolicy policy);

    public IRowCheckResult getCheckResult();
    public void setCheckResult(IRowCheckResult result);
    
}
