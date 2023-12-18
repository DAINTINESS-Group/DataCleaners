package model;

import java.util.ArrayList;

 import utils.DomainType;
 import utils.DomainTypeSettings;
 import utils.DomainValueSettings;
 import utils.ForeignKeySettings;
 import utils.NotNullSettings;
 import utils.NumberConstraintSettings;
import utils.VioletingRowPolicy;

public class ClientRequest {

    private String targetDataset;
    private ArrayList<ForeignKeySettings> foreignKeyChecks;
    private ArrayList<DomainTypeSettings> domainTypeChecks;
    private ArrayList<DomainValueSettings> domainValueChecks;
    private ArrayList<NotNullSettings> notNullChecks;
    private ArrayList<NumberConstraintSettings> numberConstraintChecks;
    private VioletingRowPolicy violationPolicy;


    private ClientRequest(Builder builder)
    {
        targetDataset = builder.targetDataset;
        foreignKeyChecks = builder.foreignKeyChecks;
        domainTypeChecks = builder.domainTypeChecks;
        domainValueChecks = builder.domainValueChecks;
        notNullChecks = builder.notNullChecks;
        numberConstraintChecks = builder.numberConstraintChecks;
        violationPolicy = builder.policy;

    }

    public String getTargetDataset() {
        return targetDataset;
    }

    public ArrayList<ForeignKeySettings> getForeignKeyChecks() {
        return foreignKeyChecks;
    }

    public ArrayList<DomainTypeSettings> getDomainTypeChecks() {
        return domainTypeChecks;
    }

    public ArrayList<DomainValueSettings> getDomainValueChecks() {
        return domainValueChecks;
    }

    public ArrayList<NotNullSettings> getNotNullChecks() {
        return notNullChecks;
    }

    public ArrayList<NumberConstraintSettings> getNumberConstraintChecks() {
        return numberConstraintChecks;
    }

    public VioletingRowPolicy getViolationPolicy()
    {
        return violationPolicy;
    }


    public static Builder builder() 
    { 
        return new Builder();
    }

    public static class Builder
    {
        String targetDataset = null;
        ArrayList<ForeignKeySettings> foreignKeyChecks = new ArrayList<ForeignKeySettings>();
        ArrayList<DomainTypeSettings> domainTypeChecks = new ArrayList<DomainTypeSettings>();
        ArrayList<DomainValueSettings> domainValueChecks = new ArrayList<DomainValueSettings>();
        ArrayList<NotNullSettings> notNullChecks = new ArrayList<NotNullSettings>();
        ArrayList<NumberConstraintSettings> numberConstraintChecks = new ArrayList<NumberConstraintSettings>();
        VioletingRowPolicy policy = VioletingRowPolicy.WARN;

        private Builder() {}

        public Builder onDataset(String datasetName)
        {
            targetDataset = datasetName;
            return this;
        }

        public Builder withForeignKeys(String targetColumn, String foreignKeyDataset, String foreignKeyColumn)
        {   
            foreignKeyChecks.add(new ForeignKeySettings(targetColumn, foreignKeyDataset, foreignKeyColumn));
            return this;
        }

        public Builder withColumnType(String targetColumn, DomainType type)
        {
            domainTypeChecks.add(new DomainTypeSettings(targetColumn, type));
            return this;
        }

        public Builder withColumnValues(String targetColumn, String[] values)
        {
            domainValueChecks.add(new DomainValueSettings(targetColumn, values));
            return this;
        }

        public Builder withNoNullValues(String targetColumn)
        {
            notNullChecks.add(new NotNullSettings(targetColumn));
            return this;
        }
        
        public Builder withNumericColumn(String targetColumn, double minValue, double maxValue,
                                          boolean includeMin, boolean includeMax)
        {
            numberConstraintChecks.add(new NumberConstraintSettings(targetColumn, minValue, maxValue, includeMin, includeMax));
            return this;
        }

        public Builder withNumericColumn(String targetColumn, double minValue, double maxValue)
        {
           return withNumericColumn(targetColumn, minValue, maxValue, true, true);
        }

        public Builder withViolationPolicy(VioletingRowPolicy policy)
        {
            this.policy = policy;
            return this;
        }

        public ClientRequest build()
        {
            return new ClientRequest(this);
        }
    }
}
