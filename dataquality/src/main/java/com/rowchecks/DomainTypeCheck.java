package com.rowchecks;

import org.apache.spark.sql.Row;

import com.utils.DomainType;


public class DomainTypeCheck implements IRowCheck {

    String targetColumn;
    DomainType type;

    public DomainTypeCheck(String targetColumn, DomainType type)
    {
        this.targetColumn = targetColumn;
        this.type = type;
    }

    //TO-DO: Upper limit on NUMERIC and INT, ALPHA. Regex?
    public CheckResult check(Row row) {
        try
        {
            String targetValue = row.getString(row.fieldIndex(targetColumn));
            switch (type)
            {
                case INTEGER:
                    Integer.parseInt(targetValue);
                    return CheckResult.PASSED;
                case BOOLEAN:
                    if (targetValue.equals("1") || targetValue.equals("0")) return CheckResult.PASSED;
                    if (targetValue.toLowerCase().equals("true") || targetValue.toLowerCase().equals("false")) return CheckResult.PASSED;
                    return CheckResult.FAILED;
                case NUMERIC:
                    Double.parseDouble(targetValue);
                    return CheckResult.PASSED;
                case ALPHA:
                    Double.parseDouble(targetValue);
                    return CheckResult.FAILED;
                default:
                    break;
            }
        }
        catch(NumberFormatException e)
        {
            if (type == DomainType.ALPHA)
            {
                return CheckResult.PASSED;
            }
        }
        catch (IllegalArgumentException e)
        {
            return CheckResult.ILLEGAL_FIELD;
        }
        catch (NullPointerException e)
        {
            return CheckResult.MISSING_VALUE;
        }
        return CheckResult.FAILED;
    }

    @Override
    public String getCheckType() {
        String ret = "Domain Type Check ";

        switch (type)
            {
                case INTEGER:
                    ret += "Integer ";
                    break;
                case BOOLEAN:
                    ret += "Boolean ";
                    break;
                case NUMERIC:
                    ret += "Numeric ";
                    break;
                case ALPHA:
                    ret += "Alphabetical ";
                    break;
                default:
                    break;
            }
        return ret + "for column: " + targetColumn;
    }
    
}
