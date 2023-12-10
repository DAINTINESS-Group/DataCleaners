package model.rowcheckresults;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import config.SparkConfig;

public abstract class GenericRowCheckResult implements IRowCheckResult, Serializable{

    final int BUFFER_SIZE = 1_000_000;

    static SparkSession spark = new SparkConfig().getSparkSession();

    private boolean hasCommitedValidRows = false;

    private Dataset<Row> approvedRows;
    private Dataset<Row> rejectedRows;
    private Dataset<Row> invalidRows;

    private ArrayList<Row> approvedRowsBuffer = new ArrayList<Row>();
    private ArrayList<Row> rejectedRowsBuffer = new ArrayList<Row>();
    private ArrayList<Row> invalidRowsBuffer = new ArrayList<Row>();

    private int approvedRowsCounter = 0;
    private int rejectedRowsCounter = 0;
    private int invalidRowsCounter = 0;

    //TODO: Can I merge these to single function with enum? Less code = better or too complex?
    public void addApprovedRow(Row row)
    {
        if (approvedRows == null) { initializeResultDatasets(row.schema()); }
        approvedRowsBuffer.add(row);
        approvedRowsCounter++;

        if (approvedRowsCounter == BUFFER_SIZE)
        {
            approvedRowsCounter = 0;
            commitApprovedRows();
        }
    }

    public void addApprovedRow(Row row, StructType schema)
    {
        if (approvedRows == null) { initializeResultDatasets(schema); }
        addApprovedRow(row);
    }

    public void addRejectedRow(Row row)
    {
        if (rejectedRows == null) { initializeResultDatasets(row.schema()); }
        rejectedRowsBuffer.add(row);
        rejectedRowsCounter++;

        if (rejectedRowsCounter == BUFFER_SIZE)
        {
            rejectedRowsCounter = 0;
            commitRejectedRows();
        }
    }

    public void addInvalidRow(Row row)
    {
        if (invalidRows == null) { initializeResultDatasets(row.schema()); }
        invalidRowsBuffer.add(row);
        invalidRowsCounter++;

        if (invalidRowsCounter == BUFFER_SIZE)
        {
            invalidRowsCounter = 0;
            commitInvalidRows();
        }
    }

    public void commitRows()
    {
        commitApprovedRows();
        commitRejectedRows();
        commitInvalidRows();
    }

    private void commitApprovedRows()
    {
        if (approvedRowsBuffer.size() == 0) return;
        hasCommitedValidRows = true;

        Dataset<Row> tempDataset = spark.createDataFrame(approvedRowsBuffer, approvedRows.schema());
        approvedRows = approvedRows.union(tempDataset);
        approvedRowsBuffer.clear();
    }

    private void commitRejectedRows()
    {
        if (rejectedRowsBuffer.size() == 0) return;
        hasCommitedValidRows = true;

        Dataset<Row> tempDataset = spark.createDataFrame(rejectedRowsBuffer, rejectedRows.schema());
        rejectedRows = rejectedRows.union(tempDataset);
        rejectedRowsBuffer.clear();
    }

    private void commitInvalidRows()
    {
        if (invalidRowsBuffer.size() == 0) return;

        Dataset<Row> tempDataset = spark.createDataFrame(invalidRowsBuffer, invalidRows.schema());
        invalidRows = invalidRows.union(tempDataset);
        invalidRowsBuffer.clear();
    }

    private void initializeResultDatasets(StructType schema)
    {
        approvedRows = spark.emptyDataFrame().to(schema);
        rejectedRows = spark.emptyDataFrame().to(schema);
        invalidRows = spark.emptyDataFrame().to(schema);
    }

    //TODO: Use this for the report.
    public String getResult() {
        return "";
    }

    public Dataset<Row> getApprovedRows()
    {
        return approvedRows;
    }
    public Dataset<Row> getRejectedRows()
    {
        return rejectedRows;
    }
    public Dataset<Row> getInvalidRows()
    {
        return invalidRows;
    }

    public boolean isSuccesful()
    {
        return hasCommitedValidRows;
    }

    public void setValidity(boolean validity)
    {
        hasCommitedValidRows = validity;
    }

}
