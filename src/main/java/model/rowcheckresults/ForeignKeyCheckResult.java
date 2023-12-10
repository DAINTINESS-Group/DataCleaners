package model.rowcheckresults;


public class ForeignKeyCheckResult extends GenericRowCheckResult{
    
    String targetColumn;
    String foreignKeyColumn;

    public ForeignKeyCheckResult(String targetColumn, String foreignKeyColumn)
    {
        this.targetColumn = targetColumn;
        this.foreignKeyColumn = foreignKeyColumn;
    }

    public String getCheckType() {
        return "Foreign Key Restriction: " +  targetColumn + "->" + foreignKeyColumn;
    }
}
