package model;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;

import rowchecks.IRowCheck;
import utils.CheckResult;
import utils.VioletingRowPolicy;

public class ServerRequest implements Serializable{
    
    //TODO: Add Hollistic and Group Checks.
    private DatasetProfile targetProfile;
    private VioletingRowPolicy violetingRowPolicy;

    private ArrayList<IRowCheck> rowChecks;
    private ServerRequestResult requestResult;

    public ServerRequest(VioletingRowPolicy violetingRowPolicy)
    {
        this.violetingRowPolicy = violetingRowPolicy;
        rowChecks = new ArrayList<IRowCheck>();
    }

    public ServerRequestResult execute()
    {
        requestResult = new ServerRequestResult();

        Dataset<Row> dataset = targetProfile.getDataset();
        Dataset<String> stringResult = dataset.map((MapFunction<Row, String>) row -> { return executeRowChecks(row); }
                                                    , Encoders.STRING());
        Dataset<Row> formattedResult = stringResult.withColumn("values", functions.split(stringResult.col("value"), ","));
        
        ArrayList<String> rowCheckTypes = new ArrayList<>();
        for (int i = 0; i < rowChecks.size(); i++)
        {
            rowCheckTypes.add(rowChecks.get(i).getCheckType());
            formattedResult = formattedResult.withColumn("c" + i, formattedResult.col("values").getItem(i));
        }
        formattedResult = formattedResult.withColumn("_id", functions.row_number().over(Window.orderBy(functions.lit("A"))));
        requestResult.applyRowCheckResults(formattedResult, rowCheckTypes);

        return requestResult;
    }

    private String executeRowChecks(Row row)
    { 
        String result = "";
        for (IRowCheck check : rowChecks)
        {
            CheckResult rowResult = check.check(row); 
            result += rowResult + ",";

            if (rowResult == CheckResult.REJECTED) { requestResult.increaseRejectedRows(); }
            else if (rowResult != CheckResult.PASSED) { requestResult.increaseInvalidRows(); }
        }
        return result.substring(0, result.length()-1);
    }

    public void addRowCheck(IRowCheck check) { rowChecks.add(check); }
    public void setProfile(DatasetProfile profile) { this.targetProfile = profile; }

    public ArrayList<IRowCheck> getRowChecks() { return rowChecks; }
    public DatasetProfile getProfile() { return targetProfile; }
    public ServerRequestResult getRequestResult() { return requestResult; }
    public VioletingRowPolicy getVioletingRowPolicy() { return violetingRowPolicy; }

    public void setRowChecks(ArrayList<IRowCheck> checks) { rowChecks = checks; }
}
