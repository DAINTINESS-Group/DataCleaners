package rowchecks.checks;

import java.io.Serializable;
import java.util.regex.Pattern;

import org.apache.spark.sql.Row;

import rowchecks.api.IRowCheck;
import utils.CheckResult;
import utils.FormatType;

/**
 * This class represents a <code>RowCheck</code> responsible for checking whether a column follows a predefined
 * format. The currently supported formats are defined by the <code>FormatType</code> enumerator. 
 * @param targetColumn The name of a column from our targeted Dataset that will be checked.
 * @param type A <code>FormatType</code> which defines the format the targetColumn must follow
 * 
 * @see FormatType
 */
public class FormatCheck implements IRowCheck, Serializable {
    private static final long serialVersionUID = 8326484956970093220L;
	private String targetColumn;
    private FormatType type;
    private Pattern regexPattern;
    

    public FormatCheck(String targetColumn, FormatType type, String delimiter) {
        this.targetColumn = targetColumn;
        this.type = type;

        String regex;
        switch (type)
        {
            case DD_MM_YYYY:
                regex = "^(0[1-9]|[12][0-9]|3[01]|[1-9])@(0[1-9]|1[012]|[1-9])@\\d+$".replace("@", delimiter);
                break;
            case MM_DD_YYYY:
                regex = "^(0[1-9]|1[012]|[1-9])@(0[1-9]|[12][0-9]|3[01]|[1-9])@\\d+$".replace("@", delimiter);
                break;
            case YYYY_MM_DD:
                regex = "^\\d+@(0[1-9]|1[012]|[1-9])@(0[1-9]|[12][0-9]|3[01]|[1-9])$".replace("@", delimiter);
                break;
            case DD_MM:
                regex = "^(0[1-9]|[12][0-9]|3[01]|[1-9])@(0[1-9]|1[012]|[1-9])$".replace("@", delimiter);
                break;
            case MM_DD:
                regex = "^(0[1-9]|1[012]|[1-9])@(0[1-9]|[12][0-9]|3[01]|[1-9])$".replace("@", delimiter);
                break;
            case YYYY_MM:
                regex = "^\\d+@(0[1-9]|1[012]|[1-9])$".replace("@", delimiter);
                break;
            case MM_YYYY:
                regex = "^(0[1-9]|1[012]|[1-9])@\\d+$".replace("@", delimiter);
                break;
            default:
                regex = "^$";
                break;
        }
        regexPattern = Pattern.compile(regex);
    }

    public CheckResult check(Row row) {
        try
        {
            String value = row.getString(row.fieldIndex(targetColumn));

            if (regexPattern.matcher(value).find())
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
        return CheckResult.REJECTED;
    }

    @Override
    public String getCheckType() {
        return "Format check " + type.toString() + " on " + targetColumn;
    }
    
}
