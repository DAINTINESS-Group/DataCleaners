package com.rowchecks;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.sql.Row;

public class DomainValuesCheck implements IRowCheck{

    ArrayList<String> domainValues;
    String targetColumn;

    public DomainValuesCheck(String targetColumn, String[] domainValues)
    {
        this.domainValues = new ArrayList<String>(Arrays.asList(domainValues));
        this.targetColumn = targetColumn;
    }

    public CheckResult check(Row row) 
    {
        try
        {
            if (domainValues.contains(row.getString(row.fieldIndex(targetColumn)))) return CheckResult.PASSED;
        }
        catch (IllegalArgumentException e)
        {
            System.out.println(row);
            return CheckResult.ILLEGAL_FIELD;
        }
        catch (NullPointerException e)
        {
            return CheckResult.MISSING_VALUE;
        }
        
        return CheckResult.FAILED;
    }

    @Override
    public String getCheckType() 
    {
        return "Domain values check for column: " + targetColumn;
    }
    
}
