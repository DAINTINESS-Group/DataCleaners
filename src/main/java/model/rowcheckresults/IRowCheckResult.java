package model.rowcheckresults;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public interface IRowCheckResult {

    public void addApprovedRow(Row row);
    public void addApprovedRow(Row row, StructType schema);
    public void addRejectedRow(Row row);
    public void addInvalidRow(Row row);
    public void commitRows();

    public Dataset<Row> getApprovedRows();
    public Dataset<Row> getRejectedRows();
    public Dataset<Row> getInvalidRows();

    public String getCheckType();
    public String getResult();

    public boolean isSuccesful();
    public void setValidity(boolean validity);
}
