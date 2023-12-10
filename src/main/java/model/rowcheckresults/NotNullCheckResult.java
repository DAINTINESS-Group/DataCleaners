package model.rowcheckresults;


public class NotNullCheckResult extends GenericRowCheckResult{
    
    String targetColumn;

    public NotNullCheckResult(String targetColumn)
    {
        this.targetColumn = targetColumn;
    }

    public String getCheckType() {
        return "Null Value Check on column: " + targetColumn;
    }
}
