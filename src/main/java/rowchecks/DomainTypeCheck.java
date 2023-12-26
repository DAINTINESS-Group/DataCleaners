package rowchecks;

import java.io.Serializable;
import java.util.regex.Pattern;

import org.apache.spark.sql.Row;

import utils.CheckResult;
import utils.DomainType;

public class DomainTypeCheck implements IRowCheck, Serializable {

    private String targetColumn;
    private DomainType type;
    private Pattern regexPattern;

    public DomainTypeCheck(String targetColumn, DomainType type)
    {
        this.targetColumn = targetColumn;
        this.type = type;

        String regex;
        switch (type)
        {
            case INTEGER:
                regex = "^\\d+$";
                break;
            case BOOLEAN:
                regex = "1|0|true|false|no|yes";
                break;
            case NUMERIC:
                regex = "(\\d+|\\d+.\\d+)";
                break;
            case ALPHA:
                regex = ".*[a-zA-Z].*";
                break;
            default:
                regex = null;
                break;
        }
        regexPattern = Pattern.compile(regex, 2);
    }

    public CheckResult check(Row row) {
        try
        {
            String targetValue = row.getString(row.fieldIndex(targetColumn)).trim();
            if (regexPattern.matcher(targetValue).find())
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
