package rowchecks;

import org.apache.spark.sql.Row;

import model.rowcheckresults.RowResultFactory;
import utils.DomainType;
import utils.VioletingRowPolicy;
import utils.CheckResult;

public class DomainTypeCheck extends GenericRowCheck {

    private String targetColumn;
    private DomainType type;

    public DomainTypeCheck(String targetColumn, DomainType type)
    {
        this.targetColumn = targetColumn;
        this.type = type;
        checkResult = new RowResultFactory().createDomainTypeCheckResult(targetColumn, type);
    }

    //TODO: Upper limit on NUMERIC and INT, ALPHA. Regex?
    public CheckResult check(Row row, VioletingRowPolicy violetingRowPolicy) {
        try
        {
            String targetValue = row.getString(row.fieldIndex(targetColumn));
            switch (type)
            {
                case INTEGER:
                    Integer.parseInt(targetValue);
                    addApprovedRow(row, violetingRowPolicy);
                    return CheckResult.PASSED;
                case BOOLEAN:
                    if (targetValue.equals("1") || targetValue.equals("0") 
                        || targetValue.toLowerCase().equals("true") 
                        || targetValue.toLowerCase().equals("false"))
                    {
                        addApprovedRow(row, violetingRowPolicy);
                        return CheckResult.PASSED;
                    }

                    addRejectedRow(row, violetingRowPolicy);
                    return CheckResult.FAILED;
                case NUMERIC:
                    Double.parseDouble(targetValue);
                    addApprovedRow(row, violetingRowPolicy);
                    return CheckResult.PASSED;
                case ALPHA:
                    Double.parseDouble(targetValue);
                    addRejectedRow(row, violetingRowPolicy);
                    return CheckResult.FAILED;
                default:
                    break;
            }
        }
        catch(NumberFormatException e)
        {
            if (type == DomainType.ALPHA)
            {
                addApprovedRow(row, violetingRowPolicy);
                return CheckResult.PASSED;
            }
        }
        catch (IllegalArgumentException e)
        {
            addInvalidRow(row, violetingRowPolicy);
            return CheckResult.ILLEGAL_FIELD;
        }
        catch (NullPointerException e)
        {
            addInvalidRow(row, violetingRowPolicy);
            return CheckResult.MISSING_VALUE;
        }
        addRejectedRow(row, violetingRowPolicy);
        return CheckResult.FAILED;
    }  
}
