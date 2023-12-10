package model.rowcheckresults;

public class DomainValuesCheckResult extends GenericRowCheckResult {

    private String targetColumn;

    public DomainValuesCheckResult(String targetColumn)
    {
        this.targetColumn = targetColumn;
    }

    public String getCheckType() 
    {
        return "Domain values check for column: " + targetColumn;
    }
}
