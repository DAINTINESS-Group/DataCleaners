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

public class UserDefinedRowCheck implements IRowCheck, Serializable{
    
    private String targetVariable;
    private String comparator;

    private String userVariable;
    private transient ScriptEngine scriptEngine;
    private HashSet<String> variableColumns = new HashSet<String>();

    public UserDefinedRowCheck(String targetVariable, String comparator, String userVariable)
    {
        this.targetVariable = targetVariable;
        this.comparator = comparator;
        this.userVariable = userVariable;

        Pattern regexPattern = Pattern.compile("[a-zA-Z]\\w*");
        Matcher matcher = regexPattern.matcher(userVariable);
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
                targetVariableValue = Double.parseDouble(targetVariable);
            }
            catch (Exception e)
            {
                targetVariableValue = Double.parseDouble(row.getString(row.fieldIndex(targetVariable)));
            }

            for (String variable : variableColumns)
            {
                scriptEngine.put(variable, Double.parseDouble(row.getString(row.fieldIndex(variable))));
            }

            boolean isCheckValid = (boolean)scriptEngine.eval(targetVariableValue+comparator+userVariable);
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
        return "UserDefined Check: " + targetVariable + " " + comparator + " " + userVariable;
    }
}
