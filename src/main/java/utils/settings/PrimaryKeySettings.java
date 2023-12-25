package utils.settings;

public class PrimaryKeySettings {
    
    private String targetColumn;

    public PrimaryKeySettings(String targetColumn)
    {
        this.targetColumn = targetColumn;
    }

    public String getTargetColumn()
    {
        return targetColumn;
    }
}
