package utils.settings;

import utils.DomainType;

public class DomainTypeSettings {
    
    String targetColumn;
    DomainType type;

    public DomainTypeSettings(String targetColumn, DomainType type)
    {
        this.targetColumn = targetColumn;
        this.type = type;
    }

    public String getTargetColumn() {
        return targetColumn;
    }

    public DomainType getType() {
        return type;
    }

    
}
