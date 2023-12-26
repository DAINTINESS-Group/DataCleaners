package model;

import java.util.ArrayList;

import utils.DomainType;
import utils.FormatType;
import utils.VioletingRowPolicy;
import utils.settings.DomainTypeSettings;
import utils.settings.DomainValueSettings;
import utils.settings.ForeignKeySettings;
import utils.settings.FormatSettings;
import utils.settings.NotNullSettings;
import utils.settings.NumberConstraintSettings;
import utils.settings.PrimaryKeySettings;

public class ClientRequest {

    private String targetDataset;
    private ArrayList<PrimaryKeySettings> primaryKeyChecks;
    private ArrayList<ForeignKeySettings> foreignKeyChecks;
    private ArrayList<FormatSettings> formatChecks;
    private ArrayList<DomainTypeSettings> domainTypeChecks;
    private ArrayList<DomainValueSettings> domainValueChecks;
    private ArrayList<NotNullSettings> notNullChecks;
    private ArrayList<NumberConstraintSettings> numberConstraintChecks;
    private VioletingRowPolicy violationPolicy;


    private ClientRequest(Builder builder)
    {
        targetDataset = builder.targetDataset;
        primaryKeyChecks = builder.primaryKeyChecks;
        foreignKeyChecks = builder.foreignKeyChecks;
        formatChecks = builder.formatChecks;
        domainTypeChecks = builder.domainTypeChecks;
        domainValueChecks = builder.domainValueChecks;
        notNullChecks = builder.notNullChecks;
        numberConstraintChecks = builder.numberConstraintChecks;
        violationPolicy = builder.policy;

    }

    public String getTargetDataset() {
        return targetDataset;
    }

    public ArrayList<PrimaryKeySettings> getPrimaryKeyChecks()
    {
        return primaryKeyChecks;
    }

    public ArrayList<ForeignKeySettings> getForeignKeyChecks() {
        return foreignKeyChecks;
    }
    
    public ArrayList<FormatSettings> getFormatChecks() {
        return formatChecks;
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
        ArrayList<PrimaryKeySettings> primaryKeyChecks = new ArrayList<PrimaryKeySettings>();
        ArrayList<ForeignKeySettings> foreignKeyChecks = new ArrayList<ForeignKeySettings>();
        ArrayList<FormatSettings> formatChecks = new ArrayList<FormatSettings>();
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

        public Builder withPrimaryKeys(String targetColumn)
        {
            primaryKeyChecks.add(new PrimaryKeySettings(targetColumn));
            return this;
        }

        public Builder withForeignKeys(String targetColumn, String foreignKeyDataset, String foreignKeyColumn)
        {   
            foreignKeyChecks.add(new ForeignKeySettings(targetColumn, foreignKeyDataset, foreignKeyColumn));
            return this;
        }

        public Builder withFormat(String targetColumn, FormatType type)
        {
            formatChecks.add(new FormatSettings(targetColumn, type, "/"));
            return this;
        }

        public Builder withFormat(String targetColumn, FormatType type, String delimeter)
        {
            formatChecks.add(new FormatSettings(targetColumn, type, delimeter));
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
