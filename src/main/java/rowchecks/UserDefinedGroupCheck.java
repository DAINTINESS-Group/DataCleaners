package rowchecks;

import java.io.Serializable;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.spark.sql.Row;

import utils.CheckResult;

public class UserDefinedGroupCheck implements IRowCheck, Serializable{

    private String conditionTargetColumn;
    private String conditionComparator;
    private String conditionUserVariable;

    private String targetColumn; 
    private String comparator;
    private String userVariable;

    private transient ScriptEngine scriptEngine;
    private HashSet<String> variableColumns = new HashSet<String>();

    //TO-DO: Consider using UserDefinedRowCheck as a condition instead of 3 vars
    public UserDefinedGroupCheck(String conditionTargetColumn, String conditionComparator, String conditionUserVariable,
                                 String targetColumn, String comparator, String userVariable)
    {
        this.conditionTargetColumn = conditionTargetColumn;
        this.conditionComparator = conditionComparator;
        this.conditionUserVariable = conditionUserVariable;

        this.targetColumn = targetColumn;
        this.comparator = comparator;
        this.userVariable = userVariable;

        Pattern regexPattern = Pattern.compile("[a-zA-Z]\\w*");
        Matcher matcher = regexPattern.matcher(conditionUserVariable);
        while (matcher.find())
        {
            variableColumns.add(matcher.group());
        }
        
        matcher = regexPattern.matcher(userVariable);
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
            boolean isCheckValid =  (boolean)scriptEngine.eval(targetColumnValue + comparator + userVariable);
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
            + " then " + targetColumn + " " + comparator + " " + userVariable;
    }
    
}
