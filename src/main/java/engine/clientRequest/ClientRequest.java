package engine.clientRequest;

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

/**
 * This class is responsible for delivering from our Client to our components, using the Facade, a number of
 * checks that are to be tested. Proper initialization of this class is through the <code> Builder </code>. Objects of
 * this class are typically translated to ServerRequests using <code>ClientToServerRequestTranslator</code> for execution.
 * @param targetDataset A string that defines the dataset's alias, registered in our application,
 *  that the client request talks about. Must always be defined and not null via {@link Builder#onDataset(String)}
 * @param violationPolicy A <code>ViolatingRowPolicy</code> that defines how rejected rows are treated when a report is
 * generated. Is <code>ViolatingRowPolicy.WARN</code> by default and can be set via {@link Builder#withViolationPolicy(ViolatingRowPolicy)}
 * @param primaryKeyChecks A group of <code>PrimaryKeySettings</code> that define <code>PrimaryKeyChecks</code> to be run on
 * the target dataset. A single check is defined via {@link Builder#withPrimaryKeys(String)}, with subsequent added with
 * each call.
 * @param foreignKeyChecks A group of <code>ForeignKeySettings</code> that define <code>ForeignKeyChecks</code> to be run on
 * the target dataset. A single check is defined via {@link Builder#withForeignKeys(String, String, String)}, with subsequent added with
 * each call.
 * @param formatChecks A group of <code>FormatSettings</code> that define <code>FormatChecks</code> to be run on
 * the target dataset. A single check is defined via {@link Builder#withFormat(String, FormatType, String)} or
 * {@link Builder#withFormat(String, FormatType)}, with subsequent added with each call.
 * @param domainTypeChecks A group of <code>DomainTypeSettings</code> that define <code>DomainTypeChecks</code> to be run on
 * the target dataset. A single check is defined via {@link Builder#withColumnType(String, DomainType)}, with subsequent added with
 * each call.
 * @param domainValuesChecks A group of <code>DomainValueSettings</code> that define <code>DomainValuesChecks</code> to be run on
 * the target dataset. A single check is defined via {@link Builder#withColumnValues(String, String[])}, with subsequent added with
 * each call.
 * @param notNullChecks A group of <code>NotNullSettings</code> that define <code>NotNullChecks</code> to be run on
 * the target dataset. A single check is defined via {@link Builder#withNoNullValues(String)}, with subsequent added with
 * each call.
 * @param numberConstraintChecks A group of <code>NumberConstraintSettings</code> that define <code>NumberConstraintChecks</code> to be run on
 * the target dataset. A single check is defined via {@link Builder#withNumericColumn(String, double, double)} or
 * {@link Builder#withNumericColumn(String, double, double, boolean, boolean)}, with subsequent added with each call.
 * @param userDefinedRowChecks A group of <code>UserDefinedRowSettings</code> that define <code>UserDefinedColumnExpressionRowChecks</code> to be run on
 * the target dataset. A single check is defined via {@link Builder#withCustomCheck(String, String, String)}, with subsequent added with each call.
 * @param userDefinedGroupChecks A group of <code>UserDefinedGroupSettings</code> that define <code>UserDefinedConditionalDependencyChecks</code> to be run on
 * the target dataset. A single check is defined via {@link Builder#withCustomConditionalCheck(String, String, String, String, String, String)}, with subsequent added with
 * each call.
 * @param userDefinedHolisticChecks A group of <code>UserDefinedHolisticSettings</code> that define <code>UserDefinedRowValueComparisonToAggValueChecks</code> to be run on
 * the target dataset. A single check is defined via {@link Builder#withCustomHollisticCheck(String, String, String)}, with subsequent added with
 * each call.
 * @see ViolatingRowPolicy
 * @see utils.settings
 * @see engine.ClientToServerRequestTranslator
 */
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
