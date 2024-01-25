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
 * The class ensures that if a condition is held in a condition column, the column is flagged OK, otherwise it is problematic.
 * 
 * Specifically, the idea is:
 * if (<conditionColumn conditionComparator conditionExpression>) then
 *    the column is flagged as OK
 * otherwise the row is flagged as problematic
 * 
 * For example:
 * if (CustomerName == 'Mitsos') then
 *     row is OK
 * meaning it is impossible for anyone to be named anything else but Mitsos 
 *       
 **/
public class UserDefinedColumnExpressionRowCheck implements IRowCheck, Serializable{
    
    private static final long serialVersionUID = 7247914239045027479L;
	private String conditionColumn;
    private String comparator;
    private String conditionExpression;
    
    private transient ScriptEngine scriptEngine;
    private HashSet<String> variableColumns = new HashSet<String>();

    public UserDefinedColumnExpressionRowCheck(String conditionColumn, String conditionComparator, String conditionExpression)
    {
        this.conditionColumn = conditionColumn;
        this.comparator = conditionComparator;
        this.conditionExpression = conditionExpression;

        Pattern regexPattern = Pattern.compile("[a-zA-Z]\\w*");
        Matcher matcher = regexPattern.matcher(conditionExpression);
        while (matcher.find())
        {
            variableColumns.add(matcher.group());
        }
    }

    public CheckResult check(Row row)
    {
        try
        {
            if (scriptEngine == null)
            {
                ScriptEngineManager manager = new ScriptEngineManager();
                scriptEngine = manager.getEngineByName("js");
            }

            double targetVariableValue;
            try
            {
                targetVariableValue = Double.parseDouble(conditionColumn);
            }
            catch (Exception e)
            {
                targetVariableValue = Double.parseDouble(row.getString(row.fieldIndex(conditionColumn)));
            }

            for (String variable : variableColumns)
            {
                scriptEngine.put(variable, Double.parseDouble(row.getString(row.fieldIndex(variable))));
            }

            boolean isCheckValid = (boolean)scriptEngine.eval(targetVariableValue+comparator+conditionExpression);
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

    public String getCheckType() 
    {
        return "UserDefined Check: " + conditionColumn + " " + comparator + " " + conditionExpression;
    }
}
