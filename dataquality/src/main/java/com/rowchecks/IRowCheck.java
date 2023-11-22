package com.rowchecks;

import org.apache.spark.sql.Row;

public interface IRowCheck {
    
    enum CheckResult
    {
        PASSED,
        FAILED,
        MISSING_VALUE,
        ILLEGAL_FIELD,
        INTERNAL_ERROR
    }

    public CheckResult check(Row row);
    public String getCheckType();
    
}
