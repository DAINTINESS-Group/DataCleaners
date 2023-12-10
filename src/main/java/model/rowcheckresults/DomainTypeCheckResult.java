package model.rowcheckresults;

import utils.DomainType;

public class DomainTypeCheckResult extends GenericRowCheckResult {

    DomainType type;
    String targetColumn;

    public DomainTypeCheckResult(String targetColumn, DomainType type)
    {
        this.type = type;
        this.targetColumn = targetColumn;
    }

    public String getCheckType() {
        String ret = "Domain Type Check ";

        switch (type)
            {
                case INTEGER:
                    ret += "Integer ";
                    break;
                case BOOLEAN:
                    ret += "Boolean ";
                    break;
                case NUMERIC:
                    ret += "Numeric ";
                    break;
                case ALPHA:
                    ret += "Alphabetical ";
                    break;
                default:
                    break;
            }
        return ret + "for column: " + targetColumn;
    }
    
}
