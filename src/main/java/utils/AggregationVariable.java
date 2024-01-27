package utils;

import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * This is a helper class that holds an aggregation variable. An aggregation variable is defined as a variable
 * within a mathematical expression used in a <code>UserDefinedRowValueComparisonToAggValueCheck</code> that makes
 * use of an aggregation function. Current aggregation functions supported: SUM, MIN, MAX, AVG
 * @param fullName The full name of the variable ex. AVG(price)
 * @param aggregation The aggregation function in string from fullName ex. AVG
 * @param variable The name of the variable from fullName ex. price
 */
public class AggregationVariable {
    private String fullName;
    private String aggregation;
    private String variable;
    public AggregationVariable(String fullName, String aggregation, String variable)
    {
        this.fullName = fullName;
        this.aggregation = aggregation;
        this.variable = variable;
    }

    public double getAggregatedValue(Dataset<Row> df) throws AnalysisException, SparkException
    {
        switch (aggregation.toUpperCase())
        {
            case "AVG":
                return df.agg(functions.avg(variable)).head().getDouble(0);
            case "SUM":
                return df.agg(functions.sum(variable)).head().getDouble(0);
            case "MIN":
                return df.agg(functions.min(df.col(variable).cast("double"))).head().getDouble(0);
            case "MAX":
                return df.agg(functions.max(df.col(variable).cast("double"))).head().getDouble(0);
        }
        return Double.NaN;
    }
    

    @Override
    public boolean equals(Object aggValue)
    {
        if (!(aggValue instanceof AggregationVariable)) return false;

        AggregationVariable castedAggValue = (AggregationVariable) aggValue;

        if (this.aggregation.equals(castedAggValue.getAggregation()) 
            && this.variable.equals(castedAggValue.getVariable()))
        {
            return true;
        }
        return false;
    }

    public String getAggregation() {
        return aggregation;
    }

    public String getVariable() {
        return variable;
    }

    public String getFullName()
    {
        return fullName;
    }
}
