package engine;

import java.util.ArrayList;

 import utils.DomainType;
 import utils.DomainTypeSettings;
 import utils.DomainValueSettings;
 import utils.ForeignKeySettings;
 import utils.NotNullSettings;
 import utils.NumberConstraintSettings;

public class QualityOrder {
    
    private final static Builder builder = new Builder();

    private String targetDataset;
    private ArrayList<ForeignKeySettings> foreignKeyChecks;
    private ArrayList<DomainTypeSettings> domainTypeChecks;
    private ArrayList<DomainValueSettings> domainValueChecks;
    private ArrayList<NotNullSettings> notNullChecks;
    private ArrayList<NumberConstraintSettings> numberConstraintChecks;


    private QualityOrder(Builder builder)
    {
        targetDataset = builder.targetDataset;
        foreignKeyChecks = builder.foreignKeyChecks;
        domainTypeChecks = builder.domainTypeChecks;
        domainValueChecks = builder.domainValueChecks;
        notNullChecks = builder.notNullChecks;
        numberConstraintChecks = builder.numberConstraintChecks;

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



    public static Builder builder() { return builder; }

    public static class Builder
    {
        String targetDataset = null;
        ArrayList<ForeignKeySettings> foreignKeyChecks = null;
        ArrayList<DomainTypeSettings> domainTypeChecks = null;
        ArrayList<DomainValueSettings> domainValueChecks = null;
        ArrayList<NotNullSettings> notNullChecks = null;
        ArrayList<NumberConstraintSettings> numberConstraintChecks = null;

        private Builder() {}

        public Builder onDataset(String datasetName)
        {
            targetDataset = datasetName;
            return this;
        }

        public Builder withForeignKeys(String targetColumn, String foreignKeyDataset, String foreignKeyColumn)
        {
            if (foreignKeyChecks == null) { foreignKeyChecks = new ArrayList<ForeignKeySettings>(); }
            
            foreignKeyChecks.add(new ForeignKeySettings(targetColumn, foreignKeyDataset, foreignKeyColumn));
            return this;
        }

        public Builder withColumnType(String targetColumn, DomainType type)
        {
            if (domainTypeChecks == null) { domainTypeChecks = new ArrayList<DomainTypeSettings>(); }

            domainTypeChecks.add(new DomainTypeSettings(targetColumn, type));
            return this;
        }

        public Builder withColumnValues(String targetColumn, String[] values)
        {
            if (domainValueChecks == null) { domainValueChecks = new ArrayList<DomainValueSettings>(); }

            domainValueChecks.add(new DomainValueSettings(targetColumn, values));
            return this;
        }

        public Builder withNoNullValues(String targetColumn)
        {
            if (notNullChecks == null) { notNullChecks = new ArrayList<NotNullSettings>(); }

            notNullChecks.add(new NotNullSettings(targetColumn));
            return this;
        }
        
        public Builder withNumericColumn(String targetColumn, double minValue, double maxValue,
                                          boolean includeMin, boolean includeMax)
        {
            if (numberConstraintChecks == null) { numberConstraintChecks = new ArrayList<NumberConstraintSettings>(); }

            numberConstraintChecks.add(new NumberConstraintSettings(targetColumn, minValue, maxValue, includeMin, includeMax));
            return this;
        }

        public Builder withNumericColumn(String targetColumn, double minValue, double maxValue)
        {
           return withNumericColumn(targetColumn, minValue, maxValue, true, true);
        }

        public QualityOrder build()
        {
            return new QualityOrder(this);
        }
    }
}
