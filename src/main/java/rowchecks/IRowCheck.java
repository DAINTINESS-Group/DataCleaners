package rowchecks;

import org.apache.spark.sql.Row;

import utils.CheckResult;

public interface IRowCheck  {

    public CheckResult check(Row row);
    public String getCheckType();
}
