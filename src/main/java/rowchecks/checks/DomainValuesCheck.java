package rowchecks.checks;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.sql.Row;

import rowchecks.api.IRowCheck;
import utils.CheckResult;

/**
 * This class represents a <code>RowCheck</code> responsible for checking whether a column contains certain
 * predefined values. This is essentially the same as a Foreign Key relationship check, but with the values being
 * manually inputted by the user.
 * 
 * @param targetColumn The name of a column from our targeted Dataset that will be checked.
 * @param domainValues A list of strings that contain the values that are allowed in the targetColumn.
 * Essentially if a value from targetColumn is not within domainValues, it is rejected.
 */
public class DomainValuesCheck implements IRowCheck, Serializable {

    private static final long serialVersionUID = 3737639305672122256L;
	private ArrayList<String> domainValues;
    private String targetColumn;

    public DomainValuesCheck(String targetColumn, String[] domainValues)
    {
        this.domainValues = new ArrayList<String>(Arrays.asList(domainValues));
        this.targetColumn = targetColumn;
    }

    public CheckResult check(Row row) 
    {
        try
        {
            if (domainValues.contains(row.getString(row.fieldIndex(targetColumn))))
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

    public String getCheckType()
    {
        return "Domain Values On " + targetColumn;
    }
}
