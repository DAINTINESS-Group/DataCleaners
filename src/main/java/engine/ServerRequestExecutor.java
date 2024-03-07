package engine;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;

import config.SparkConfig;
import model.ServerRequest;
import model.ServerRequestResult;
import rowchecks.api.IRowCheck;
import utils.CheckResult;

/**
 * This class is responsible for executing a <code>ServerRequest</code> and producing a <code>ServerRequestResult</code>
 * which it stores within the <code>ServerRequest</code>.
 * @see ServerRequest
 * @see ServerRequestResult
 */
public class ServerRequestExecutor implements Serializable {
    
    private static final long serialVersionUID = 7963000311509095105L;
	private ArrayList<IRowCheck> rowChecks;
    private ServerRequestResult requestResult;

    private LongAccumulator rejectedRows = new SparkConfig().getSparkSession().sparkContext().longAccumulator("longAccRejected");
    private LongAccumulator invalidRows = new SparkConfig().getSparkSession().sparkContext().longAccumulator("longAccInvalid");

    public ServerRequestResult executeServerRequest(ServerRequest request)
    {
        requestResult = new ServerRequestResult();
        rowChecks = request.getRowChecks();

        Dataset<Row> dataset = request.getProfile().getDataset().persist();

        Dataset<Row> rowCheckResults = dataset.map((MapFunction<Row, Row>) row -> { return executeRowChecks(row); }
                                                    , Encoders.bean(Row.class));
        
        StructField[] structs = new StructField[rowChecks.size()];
        ArrayList<String> rowCheckTypes = new ArrayList<>();
        for (int i = 0; i < rowChecks.size(); i++)
        {
            rowCheckTypes.add(rowChecks.get(i).getCheckType());
            structs[i] = DataTypes.createStructField("c"+i, DataTypes.StringType, true);
        }
        StructType schema = DataTypes.createStructType(structs);
        rowCheckResults = new SparkConfig().getSparkSession().createDataFrame(rowCheckResults.rdd(), schema); 
        
        rowCheckResults = rowCheckResults.withColumn("_id", functions.row_number().over(Window.orderBy(functions.lit("A")))).persist(StorageLevel.DISK_ONLY());
    
        rowCheckResults.count(); //Prompt execution
        requestResult.applyRowCheckResults(rowCheckResults, rowCheckTypes, rejectedRows.value(), invalidRows.value());
        request.setRequestResult(requestResult);
        rejectedRows.reset();
        invalidRows.reset();
        return requestResult;
    }

    private Row executeRowChecks(Row row)
    { 
        String[] result = new String[rowChecks.size()];
        boolean isRejected = false;
        boolean isInvalid = false;
        for (int i = 0; i < rowChecks.size(); i++)
        {
            CheckResult rowResult = rowChecks.get(i).check(row); 
            result[i] = rowResult.toString();

            if (rowResult == CheckResult.REJECTED) { isRejected = true; }
            else if (rowResult != CheckResult.PASSED) { isInvalid = true; }
        }

        if (isRejected) rejectedRows.add(1);
        if (isInvalid) invalidRows.add(1);
        return RowFactory.create((Object[])result);
    }
}
