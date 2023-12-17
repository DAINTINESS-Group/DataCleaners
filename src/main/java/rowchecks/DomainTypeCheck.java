package rowchecks;

import java.io.Serializable;

import org.apache.spark.sql.Row;

import utils.DomainType;
import utils.CheckResult;

public class DomainTypeCheck implements IRowCheck, Serializable {

    private String targetColumn;
    private DomainType type;

    public DomainTypeCheck(String targetColumn, DomainType type)
    {
        this.targetColumn = targetColumn;
        this.type = type;
    }

    //TODO: Upper limit on NUMERIC and INT, ALPHA. Regex?
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
                    if (targetValue.equals("1") || targetValue.equals("0") 
                        || targetValue.toLowerCase().equals("true") 
                        || targetValue.toLowerCase().equals("false"))
                    {
                        return CheckResult.PASSED;
                    }

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

    public String getCheckType()
    {
        return "DomainTypeCheck On " + targetColumn + ": " + type.toString();
    }
}
