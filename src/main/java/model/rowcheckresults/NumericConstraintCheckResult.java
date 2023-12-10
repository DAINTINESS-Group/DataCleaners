package model.rowcheckresults;

public class NumericConstraintCheckResult extends GenericRowCheckResult{

    private String targetColumn;
    private double minValue;
    private boolean isLeftInclusive;
    private double maxValue;
    private boolean isRightInclusive;

    public NumericConstraintCheckResult(String targetColumn, double minValue, double maxValue,
                                        boolean isLeftInclusive, boolean isRightInclusive)
    {
        this.targetColumn = targetColumn;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.isLeftInclusive = isLeftInclusive;
        this.isRightInclusive = isRightInclusive;
    }

    public String getCheckType() {
        return "Numeric Constraint Check: " + getConstraint() ;
    }

    private String getConstraint()
    {
        String ret = isLeftInclusive ? "[" : "(";
        ret += minValue + ", " + maxValue;
        ret += isRightInclusive ? "]" : ")";
        return ret + " for column: " + targetColumn;
    }
}
