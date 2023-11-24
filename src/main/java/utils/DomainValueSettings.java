package utils;

public class DomainValueSettings {
    
    String targetColumn;
    String[] values;
    
    public DomainValueSettings(String targetColumn, String[] values) {
        this.targetColumn = targetColumn;
        this.values = values;
    }

    public String getTargetColumn() {
        return targetColumn;
    }

    public String[] getValues() {
        return values;
    }

    
}
