package utils.settings;

public class NumberConstraintSettings {
    
    String targetColumn;
    double minValue;
    double maxValue;
    boolean includeMinimum;
    boolean includeMaximum;
    
    public NumberConstraintSettings(String targetColumn, double minValue, double maxValue, boolean includeMinimum,
            boolean includeMaximum) {
        this.targetColumn = targetColumn;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.includeMinimum = includeMinimum;
        this.includeMaximum = includeMaximum;
    }

    public String getTargetColumn() {
        return targetColumn;
    }

    public double getMinValue() {
        return minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public boolean isIncludeMinimum() {
        return includeMinimum;
    }

    public boolean isIncludeMaximum() {
        return includeMaximum;
    }

    
}
