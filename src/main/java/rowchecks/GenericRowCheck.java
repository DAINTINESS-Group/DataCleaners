package rowchecks;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import model.rowcheckresults.IRowCheckResult;
import utils.VioletingRowPolicy;

public abstract class GenericRowCheck implements IRowCheck, Serializable {
    
    protected IRowCheckResult checkResult;

    public IRowCheckResult getCheckResult()
    {
        checkResult.commitRows();
        return checkResult;
    }

    public void setCheckResult(IRowCheckResult result)
    {
        checkResult = result;
    }

    protected void addApprovedRow(Row row, VioletingRowPolicy policy)
    {
        if (policy == VioletingRowPolicy.WARN)
        {
            addRowWithWarning(row, "OK");
            return;
        }
        checkResult.addApprovedRow(row);
    }

    protected void addRejectedRow(Row row, VioletingRowPolicy policy)
    {
        if (policy == VioletingRowPolicy.PURGE) return;
        if (policy == VioletingRowPolicy.WARN)
        {
            addRowWithWarning(row, checkResult.getCheckType());
            return;
        }
        checkResult.addRejectedRow(row);
    }

    protected void addInvalidRow(Row row, VioletingRowPolicy policy)
    {
        if (policy == VioletingRowPolicy.WARN)
        {
            addRowWithWarning(row, "ERROR: Check Failed due to internal or format error.");
            return;
        }
        checkResult.addInvalidRow(row);
    }

    private void addRowWithWarning(Row row, String warning)
    {
        StructType schema = row.schema();
        String rowString = row.toString();
        List<String> splitRow = new ArrayList<String>();
        for (String s : rowString.substring(1, rowString.length()-1).split(","))
        {
            splitRow.add(s);
        }
        splitRow.add(warning);

        Row rowWithWarning = RowFactory.create(splitRow.toArray());
        checkResult.addApprovedRow(rowWithWarning, schema.add("_warning","STRING"));
    }
}
