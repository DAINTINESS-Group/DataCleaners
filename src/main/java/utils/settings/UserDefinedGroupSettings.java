package utils.settings;

public class UserDefinedGroupSettings {

    private String conditionTargetColumn;
    private String conditionComparator;
    private String conditionUserVariable;

    private String targetColumn; 
    private String comparator;
    private String userVariable;

    public UserDefinedGroupSettings(String conditionTargetColumn, String conditionComparator,
            String conditionUserVariable, String targetColumn, String comparator, String userVariable) {
        this.conditionTargetColumn = conditionTargetColumn;
        this.conditionComparator = conditionComparator;
        this.conditionUserVariable = conditionUserVariable;
        this.targetColumn = targetColumn;
        this.comparator = comparator;
        this.userVariable = userVariable;
    }

    public String getConditionTargetColumn() {
        return conditionTargetColumn;
    }

    public String getConditionComparator() {
        return conditionComparator;
    }

    public String getConditionUserVariable() {
        return conditionUserVariable;
    }

    public String getTargetColumn() {
        return targetColumn;
    }

    public String getComparator() {
        return comparator;
    }

    public String getUserVariable() {
        return userVariable;
    }

    

    
}
