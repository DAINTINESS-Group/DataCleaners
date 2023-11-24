package utils;

public class ForeignKeySettings {
    
    String targetColumn;
    String foreignKeyDataset;
    String foreignKeyColumn;

    public ForeignKeySettings(String targetColumn, String foreignKeyDataset, String foreignKeyColumn)
    {
        this.targetColumn = targetColumn;
        this.foreignKeyColumn = foreignKeyColumn;
        this.foreignKeyDataset = foreignKeyDataset;
    }

    public String getTargetColumn() {
        return targetColumn;
    }

    public String getForeignKeyDataset() {
        return foreignKeyDataset;
    }

    public String getForeignKeyColumn() {
        return foreignKeyColumn;
    }

    
}
