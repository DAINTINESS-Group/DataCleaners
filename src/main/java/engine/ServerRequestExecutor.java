package engine;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;

import model.ServerRequest;
import model.ServerRequestResult;
import rowchecks.api.IRowCheck;
import utils.CheckResult;

public class ServerRequestExecutor implements Serializable {
    
    private static final long serialVersionUID = 7963000311509095105L;
	private ArrayList<IRowCheck> rowChecks;
    private ServerRequestResult requestResult;

    public ServerRequestResult executeServerRequest(ServerRequest request)
    {
        requestResult = new ServerRequestResult();
        rowChecks = request.getRowChecks();

        Dataset<Row> dataset = request.getProfile().getDataset();
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
        
        request.setRequestResult(requestResult);
        
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
}
