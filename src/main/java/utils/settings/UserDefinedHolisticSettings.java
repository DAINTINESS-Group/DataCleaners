package utils.settings;

public class UserDefinedHolisticSettings {
    
    private String targetColumn;
    private String comparator;
    private String userVariable;
    
    public UserDefinedHolisticSettings(String targetColumn, String comparator, String userVariable) {
        this.targetColumn = targetColumn;
        this.comparator = comparator;
        this.userVariable = userVariable;
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
