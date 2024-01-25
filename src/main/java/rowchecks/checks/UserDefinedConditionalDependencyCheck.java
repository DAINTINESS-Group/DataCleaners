package rowchecks.checks;

import java.io.Serializable;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.spark.sql.Row;

import rowchecks.api.IRowCheck;
import utils.CheckResult;

/**
 * THe class ensures that if a condition is held in a condition column, a respective target condition is held in a target column.
 * 
 * Specifically, the idea is:
 * if (<conditionColumn conditionComparator conditionExpression>) then
 *    <(targetColumn targetComparator targetExpression> must also hold
 * otherwise the row is flagged as problematic
 * The row is flagged as OK if the first condition check doees not hold
 * 
 * For example:
 * if (CustomerName == 'Mitsos') then
 *     (CustomaerAge < 45)
 * meaning it is impossible for anyone named mitsos to be older than 45
 *       
 */
public class UserDefinedConditionalDependencyCheck implements IRowCheck, Serializable{

    private static final long serialVersionUID = 1L;
	private String conditionTargetColumn;
    private String conditionComparator;
    private String conditionUserVariable;

    private String targetColumn; 
    private String targetComparator;
    private String targetExpression;

    private transient ScriptEngine scriptEngine;
    private HashSet<String> variableColumns = new HashSet<String>();


    public UserDefinedConditionalDependencyCheck(String conditionColumn, String conditionComparator, String conditionExpression,
                                 String targetColumn, String targetComparator, String targetExpression)
    //TO-DO: Consider using UserDefinedColumnExpressionRowCheck as a condition instead of 3 vars
    {
        this.conditionTargetColumn = conditionColumn;
        this.conditionComparator = conditionComparator;
        this.conditionUserVariable = conditionExpression;

        this.targetColumn = targetColumn;
        this.targetComparator = targetComparator;
        this.targetExpression = targetExpression;

        Pattern regexPattern = Pattern.compile("[a-zA-Z]\\w*");
        Matcher matcher = regexPattern.matcher(conditionExpression);
        while (matcher.find())
        {
            variableColumns.add(matcher.group());
        }
        
        matcher = regexPattern.matcher(targetExpression);
        while (matcher.find())
        {
            variableColumns.add(matcher.group());
        }
    }

    public CheckResult check(Row row) {
        try
        {
            if (scriptEngine == null)
            {
                ScriptEngineManager manager = new ScriptEngineManager();
                scriptEngine = manager.getEngineByName("js");
            }

            //Calculate all variable values
            for (String variable : variableColumns)
            {
                scriptEngine.put(variable, Double.parseDouble(row.getString(row.fieldIndex(variable))));
            }

            double conditionTargetColumnValue = Double.parseDouble(row.getString(row.fieldIndex(conditionTargetColumn)));
            boolean doesConditionApply = (boolean)scriptEngine.eval(conditionTargetColumnValue + conditionComparator + conditionUserVariable);
            //If the condition is false, we can ignore this row.
            if (!doesConditionApply) return CheckResult.PASSED;

            //If the condition is true, we now can check the second condition.
            double targetColumnValue = Double.parseDouble(row.getString(row.fieldIndex(targetColumn)));
            boolean isCheckValid =  (boolean)scriptEngine.eval(targetColumnValue + targetComparator + targetExpression);
            return isCheckValid ? CheckResult.PASSED : CheckResult.REJECTED;
        }
        catch(NullPointerException e)
        {
            return CheckResult.MISSING_VALUE;
        }
        catch (ScriptException e)
        {
            return CheckResult.ILLEGAL_FIELD;
        }
        catch (IllegalArgumentException e)
        {
            return CheckResult.ILLEGAL_FIELD;
        }
    }

    @Override
    public String getCheckType() {
        return "UserDefined Check If " + conditionTargetColumn + " " + conditionComparator + " " + conditionUserVariable 
            + " then " + targetColumn + " " + targetComparator + " " + targetExpression;
    }
    
}
