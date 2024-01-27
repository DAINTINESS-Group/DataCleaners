package rowchecks.checks;

import java.io.Serializable;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.Row;

import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import rowchecks.api.IRowCheck;
import utils.CheckResult;
import utils.Comparator;

/**
 * THe class ensures that if a condition is held in a condition column, a respective target condition is held in a target column.
 * 
 * Specifically, the idea is:
 * <pre>
 * if (conditionColumn conditionComparator conditionExpression) then
 *    (targetColumn targetComparator targetExpression>) must also hold
 * </pre>
 * otherwise the row is flagged as problematic
 * The row is flagged as OK if the first condition check doees not hold
 * 
 * For example:
 * <pre>
 * if (CustomerName == 'Mitsos') then
 *     if (CustomaerAge < 45) then
 *       row is PASSED
 *     else
 *      row is REJECTED
 * else
 *  row is PASSED
 * </pre>
 * meaning it is impossible for anyone named mitsos to be older than 45
 *  
 *  @param conditionTargetColumn A column name or number
 *  @param conditionComparator A symbol used in comparison ex >, <, ==, >=
 *  @param conditionExpression A mathematical expression that can contain columns
 * 
 *  @param targetColumn A column name or number
 *  @param targetComparator A symbol used in comparison ex >, <, ==, >=
 *  @param targetExpression A mathematical expression that can contain columns
 */
public class UserDefinedConditionalDependencyCheck implements IRowCheck, Serializable{

    private static final long serialVersionUID = 1L;
	private String conditionTargetColumn;
    private String conditionComparator;
    private String conditionExpression;

    private String targetColumn; 
    private String targetComparator;
    private String targetExpression;

    private transient Expression conditionEvaluator;
    private transient Expression targetEvaluator;
    private HashSet<String> variableColumns = new HashSet<String>();


    public UserDefinedConditionalDependencyCheck(String conditionColumn, String conditionComparator, String conditionExpression,
                                 String targetColumn, String targetComparator, String targetExpression)
    //TO-DO: Consider using UserDefinedColumnExpressionRowCheck as a condition instead of 3 vars
    {
        this.conditionTargetColumn = conditionColumn;
        this.conditionComparator = conditionComparator;
        this.conditionExpression = conditionExpression;

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
            if (conditionEvaluator == null || targetEvaluator == null)
            {
                ExpressionBuilder conditionEvalBuilder = new ExpressionBuilder(conditionExpression);
                ExpressionBuilder targetEvalBuilder = new ExpressionBuilder(targetExpression);

                for (String variable : variableColumns)
                {
                    conditionEvalBuilder = conditionEvalBuilder.variable(variable);
                    targetEvalBuilder = targetEvalBuilder.variable(variable);
                }

                conditionEvaluator = conditionEvalBuilder.build();
                targetEvaluator = targetEvalBuilder.build();
            }

            //Calculate all variable values
            for (String variable : variableColumns)
            {
                double varValue = Double.parseDouble(row.getString(row.fieldIndex(variable)));
                conditionEvaluator.setVariable(variable, varValue);
                targetEvaluator.setVariable(variable, varValue);
            }

            double conditionTargetColumnValue;
            try
            {
                conditionTargetColumnValue = Double.parseDouble(conditionTargetColumn);
            }
            catch (Exception e)
            {
                conditionTargetColumnValue = Double.parseDouble(row.getString(row.fieldIndex(conditionTargetColumn)));
            }
            double conditionExpressionValue = conditionEvaluator.evaluate();

            boolean doesConditionApply = Comparator.compareValues(conditionTargetColumnValue, conditionComparator, conditionExpressionValue);
            //If the condition is false, we can ignore this row.
            if (!doesConditionApply) return CheckResult.PASSED;

            //If the condition is true, we now can check the second condition.
            double targetColumnValue;
            try
            {
                targetColumnValue = Double.parseDouble(targetColumn);
            }
            catch (Exception e)
            {
                targetColumnValue = Double.parseDouble(row.getString(row.fieldIndex(targetColumn)));
            }
            double targetExpressionValue = targetEvaluator.evaluate();
            boolean isCheckValid = Comparator.compareValues(targetColumnValue, targetComparator, targetExpressionValue);
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

    @Override
    public String getCheckType() {
        return "UserDefined Check If " + conditionTargetColumn + " " + conditionComparator + " " + conditionExpression 
            + " then " + targetColumn + " " + targetComparator + " " + targetExpression;
    }
    
}
