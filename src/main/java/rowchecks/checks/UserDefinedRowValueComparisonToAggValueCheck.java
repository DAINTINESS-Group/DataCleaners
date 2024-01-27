package rowchecks.checks;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import rowchecks.api.IRowCheck;
import utils.AggregationVariable;
import utils.CheckResult;
import utils.Comparator;


/**
 * The class ensures that if a condition is held in a condition column, the column is flagged OK, otherwise it is problematic.
 * THe condition relates the value of the current row to an aggregate statistic over the column
 * 
 * Specifically, the idea is:
 * if (<conditionColumn conditionComparator conditionExpression>) then
 *    the column is flagged as OK
 * otherwise the row is flagged as problematic
 * 
 * For example:
 * if (EmplSalary >= AVG(EmpSalary)+145*7 ) then
 *     row is OK
 * meaning we flag as problematic all employees with salary lower than AVG(EmpSalary)+145*7  
 *       
 **/

public class UserDefinedRowValueComparisonToAggValueCheck implements IRowCheck, Serializable{
    
    private static final long serialVersionUID = 8227700374246743115L;
	private String conditionColumn;
    private String comparator;
    private String conditionExpression;
    private String translatedUserVariable;

    private HashSet<String> variableColumns;
    private HashMap<String, Double> aggregatedColumns;

    private transient Expression evaluator;

    public UserDefinedRowValueComparisonToAggValueCheck(String conditionColumn, String comparator, String conditionExpression,
                                    Dataset<Row> targetDataset)
    {
        this.conditionColumn = conditionColumn;
        this.comparator = comparator;
        this.conditionExpression = conditionExpression;

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
        Matcher matcher = regexPattern.matcher(conditionExpression);
        while (matcher.find())
        {
            String match = matcher.group();
            if (!aggregationKeywords.contains(match.toUpperCase())) variableColumns.add(match);
        }
        
        regexPattern = Pattern.compile("(?i)(SUM|AVG|MAX|MIN)\\([a-zA-Z]\\w*\\)");
        matcher = regexPattern.matcher(conditionExpression);
        String translatedUserVariable = conditionExpression;
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
            try
            {
                String translatedName = aggregationVariableTranslator.get(av.getFullName());
                aggregatedColumns.put(translatedName, av.getAggregatedValue(targetDataset));
            }
            catch (Exception e)
            {
                //Aggregated column is missing/invalid. Let execution throw IllegalArgument by not including
                //the variable for the ScriptEngine.
                continue;
            }
        }
        this.translatedUserVariable = translatedUserVariable;
    }

    public CheckResult check(Row row) {
        try
        {
            if (evaluator == null)
            {
                ExpressionBuilder builder = new ExpressionBuilder(translatedUserVariable);
                for (String key : aggregatedColumns.keySet())
                {
                    builder = builder.variable(key);
                }
                for (String var : variableColumns)
                {
                    builder = builder.variable(var);
                }

                evaluator = builder.build();
                for (String key : aggregatedColumns.keySet())
                {
                    evaluator.setVariable(key, aggregatedColumns.get(key));
                }
            }
            
            //Determine if conditionColumn is a number or a column.
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
                evaluator.setVariable(variable, Double.parseDouble(row.getString(row.fieldIndex(variable))));
            }

            double conditionExpressionValue = evaluator.evaluate();
            boolean isCheckValid = Comparator.compareValues(targetVariableValue, comparator, conditionExpressionValue);
            return isCheckValid ? CheckResult.PASSED : CheckResult.REJECTED;
        }
        catch(NullPointerException e)
        {
            return CheckResult.MISSING_VALUE;
        }
        catch (IllegalArgumentException e)
        {
            return CheckResult.ILLEGAL_FIELD;
        }
    }

    public String getCheckType() {
        return "UserDefined Check: " + conditionColumn + " " + comparator + " " + conditionExpression;
    }
    
}
