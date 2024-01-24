package model;

import java.util.ArrayList;

import utils.DomainType;
import utils.FormatType;
import utils.ViolatingRowPolicy;
import utils.settings.DomainTypeSettings;
import utils.settings.DomainValueSettings;
import utils.settings.ForeignKeySettings;
import utils.settings.FormatSettings;
import utils.settings.NotNullSettings;
import utils.settings.NumberConstraintSettings;
import utils.settings.PrimaryKeySettings;
import utils.settings.UserDefinedGroupSettings;
import utils.settings.UserDefinedHolisticSettings;
import utils.settings.UserDefinedRowSettings;

public class ClientRequest {

    private String targetDataset;
    private ArrayList<PrimaryKeySettings> primaryKeyChecks;
    private ArrayList<ForeignKeySettings> foreignKeyChecks;
    private ArrayList<FormatSettings> formatChecks;
    private ArrayList<DomainTypeSettings> domainTypeChecks;
    private ArrayList<DomainValueSettings> domainValueChecks;
    private ArrayList<NotNullSettings> notNullChecks;
    private ArrayList<NumberConstraintSettings> numberConstraintChecks;

    private ArrayList<UserDefinedRowSettings> userDefinedRowChecks;
    private ArrayList<UserDefinedGroupSettings> userDefinedGroupChecks;
    private ArrayList<UserDefinedHolisticSettings> userDefinedHolisticChecks;
    private ViolatingRowPolicy violationPolicy;


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
        userDefinedRowChecks = builder.userDefinedRowChecks;
        userDefinedGroupChecks = builder.userDefinedGroupChecks;
        userDefinedHolisticChecks = builder.userDefinedHolisticChecks;
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

    public ArrayList<UserDefinedRowSettings> getUserDefinedRowSettings()
    {
        return userDefinedRowChecks;
    }

    public ArrayList<UserDefinedGroupSettings> getUserDefinedGroupSettings()
    {
        return userDefinedGroupChecks;
    }

    public ArrayList<UserDefinedHolisticSettings> getUserDefinedHolisticSettings()
    {
        return userDefinedHolisticChecks;
    }

    public ViolatingRowPolicy getViolationPolicy()
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
        ArrayList<UserDefinedRowSettings> userDefinedRowChecks = new ArrayList<UserDefinedRowSettings>();
        ArrayList<UserDefinedGroupSettings> userDefinedGroupChecks = new ArrayList<UserDefinedGroupSettings>();
        ArrayList<UserDefinedHolisticSettings> userDefinedHolisticChecks = new ArrayList<UserDefinedHolisticSettings>();
        ViolatingRowPolicy policy = ViolatingRowPolicy.WARN;

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

        public Builder withCustomCheck(String targetColumn, String comparator, String customVariable)
        {
            userDefinedRowChecks.add(new UserDefinedRowSettings(targetColumn, comparator, customVariable));
            return this;
        }

        public Builder withCustomConditionalCheck(String conditionTargetColumn, String conditionComparator,
                    String conditionUserVariable, String targetColumn, String comparator, String userVariable)
        {
            userDefinedGroupChecks.add(new UserDefinedGroupSettings(conditionTargetColumn, conditionComparator, 
                                                    conditionUserVariable, targetColumn, comparator, userVariable));
            return this;
        }

        public Builder withCustomHollisticCheck(String targetColumn, String comparator, String customVariable)
        {
            userDefinedHolisticChecks.add(new UserDefinedHolisticSettings(targetColumn, comparator, customVariable));
            return this;
        }

        public Builder withViolationPolicy(ViolatingRowPolicy policy)
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
