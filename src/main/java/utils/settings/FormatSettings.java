package utils.settings;

import utils.FormatType;

public class FormatSettings {
    
    private String targetColumn;
    private FormatType type;
    private String delimeter;

    public FormatSettings(String targetColumn, FormatType type, String delimeter)
    {
        this.targetColumn = targetColumn;
        this.type = type;
        this.delimeter = delimeter;
    }

    public String getTargetColumn() {
        return targetColumn;
    }

    public FormatType getType() {
        return type;
    }

    public String getDelimeter()
    {
        return delimeter;
    }

    
}
