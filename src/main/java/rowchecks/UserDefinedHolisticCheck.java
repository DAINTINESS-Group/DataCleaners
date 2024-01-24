package rowchecks;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import utils.AggregationVariable;
import utils.CheckResult;

public class UserDefinedHolisticCheck implements IRowCheck, Serializable{
    
    private String targetVariable;
    private String comparator;
    private String userVariable;
    private String translatedUserVariable;

    private HashSet<String> variableColumns;
    private HashMap<String, Double> aggregatedColumns;

    private transient ScriptEngine scriptEngine;

    public UserDefinedHolisticCheck(String targetVariable, String comparator, String userVariable,
                                    Dataset<Row> targetDataset)
    {
        this.targetVariable = targetVariable;
        this.comparator = comparator;
        this.userVariable = userVariable;

        aggregatedColumns = new HashMap<String, Double>();
        variableColumns = new HashSet<String>();
        ArrayList<AggregationVariable> aggregationVariables = new ArrayList<AggregationVariable>();

        ArrayList<String> aggregationKeywords = new ArrayList<String>();
        aggregationKeywords.add("SUM");
        aggregationKeywords.add("MIN");
        aggregationKeywords.add("MAX");
        aggregationKeywords.add("AVG");

        //Step 1: Isolate all simple variables. NOTE: This also fetches aggregation keywords!
        Pattern regexPattern = Pattern.compile("[a-zA-Z]\\w*");
        Matcher matcher = regexPattern.matcher(userVariable);
        while (matcher.find())
        {
            String match = matcher.group();
            if (!aggregationKeywords.contains(match.toUpperCase())) variableColumns.add(match);
        }
        
        regexPattern = Pattern.compile("(?i)(SUM|AVG|MAX|MIN)\\([a-zA-Z]\\w*\\)");
        matcher = regexPattern.matcher(userVariable);
        String translatedUserVariable = userVariable;
        int counter = 0;
        HashMap<String, String> aggregationVariableTranslator = new HashMap<String,String>();
        //Find all aggregation functions, translate them to single variables.
        while (matcher.find())
        {
            String match = matcher.group();
            if (!aggregationVariableTranslator.containsKey(match))
            {
                aggregationVariableTranslator.put(match, "_x"+counter);
                translatedUserVariable = translatedUserVariable.replace(match, "_x"+counter);
                counter++;
            }

            String aggregationFunction;
            String variableName;

            aggregationFunction = match.substring(0,3); 
            variableName = match.substring(4, match.length()-1);
            
            
            AggregationVariable aggrVar = new AggregationVariable(match, aggregationFunction, variableName);
            if (!aggregationVariables.contains(aggrVar))
            {
                aggregationVariables.add(aggrVar);
            }
        }

        for (AggregationVariable av : aggregationVariables)
        {
            String translatedName = aggregationVariableTranslator.get(av.getFullName());
            aggregatedColumns.put(translatedName, av.getAggregatedValue(targetDataset));
        }
        this.translatedUserVariable = translatedUserVariable;
    }

    public CheckResult check(Row row) {
        try
        {
            if (scriptEngine == null)
            {
                ScriptEngineManager manager = new ScriptEngineManager();
                scriptEngine = manager.getEngineByName("js");

                for (String key : aggregatedColumns.keySet())
                {
                    scriptEngine.put(key, aggregatedColumns.get(key));
                }
            }
            
            //Determine if targetVariable is a number or a column.
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

            boolean isCheckValid = (boolean)scriptEngine.eval(targetVariableValue+comparator+translatedUserVariable);
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

    public String getCheckType() {
        return "UserDefined Check: " + targetVariable + " " + comparator + " " + userVariable;
    }
    
}
