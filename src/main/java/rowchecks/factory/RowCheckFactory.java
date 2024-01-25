package rowchecks.factory;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import model.DatasetProfile;
import rowchecks.api.IRowCheck;
import rowchecks.checks.BPlusTreeForeignKeyCheck;
import rowchecks.checks.BPlusTreePrimaryKeyCheck;
import rowchecks.checks.DomainTypeCheck;
import rowchecks.checks.DomainValuesCheck;
import rowchecks.checks.FormatCheck;
import rowchecks.checks.NotNullCheck;
import rowchecks.checks.NumericConstraintCheck;
import rowchecks.checks.UserDefinedConditionalDependencyCheck;
import rowchecks.checks.UserDefinedRowValueComparisonToAggValueCheck;
import rowchecks.checks.UserDefinedColumnExpressionRowCheck;
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

//TODO add defensive code for null parameters
public class RowCheckFactory {

	ArrayList<DatasetProfile> profiles;
	public RowCheckFactory(ArrayList<DatasetProfile> profiles)
	{
		this.profiles = profiles;
	}

	public IRowCheck createPrimaryKeyCheck(PrimaryKeySettings pkSettings)
	{
		return new BPlusTreePrimaryKeyCheck(pkSettings.getTargetColumn());
	}

	public IRowCheck createFormatCheck(FormatSettings fSettings)
	{
		return new FormatCheck(fSettings.getTargetColumn(), fSettings.getType(), fSettings.getDelimeter());
	}

	public IRowCheck createDomainTypeCheck(DomainTypeSettings dtSettings) {
		return new 	DomainTypeCheck(dtSettings.getTargetColumn(), dtSettings.getType());
	}

	public IRowCheck createDomainValuesCheck(DomainValueSettings dvSettings) {
		return new DomainValuesCheck(dvSettings.getTargetColumn(), dvSettings.getValues());
	}
	
	public IRowCheck createNotNullCheck(NotNullSettings nnSettings) {
		return new NotNullCheck(nnSettings.getTargetColumn());
	}
	
	public IRowCheck createNumericConstraintCheck(NumberConstraintSettings ncSettings) {
		return new NumericConstraintCheck(ncSettings.getTargetColumn(),
										ncSettings.getMinValue(), ncSettings.getMaxValue(),
										ncSettings.isIncludeMinimum(), ncSettings.isIncludeMaximum());
	}
	
	public IRowCheck createBTreeForeignKeyCheck(ForeignKeySettings fkSettings) {
		
		Dataset<Row> df = null;
		for (DatasetProfile profile : profiles)
		{
			if (profile.getAlias().equals(fkSettings.getForeignKeyDataset()))
			{
				df = profile.getDataset();
				break;
			}
		}

		return new BPlusTreeForeignKeyCheck(fkSettings.getTargetColumn(), 
										df,
										fkSettings.getForeignKeyColumn());
	}

	public IRowCheck createUserDefinedCheck(UserDefinedRowSettings udSettings)
	{
		return new UserDefinedColumnExpressionRowCheck(udSettings.getTargetColumn(),
									   udSettings.getComparator(),
									   udSettings.getUserVariable());
	}

	public IRowCheck createUserDefinedGroupCheck(UserDefinedGroupSettings udgSettings)
	{
		return new UserDefinedConditionalDependencyCheck(udgSettings.getConditionTargetColumn(),
										 udgSettings.getConditionComparator(),
										 udgSettings.getConditionUserVariable(),
										 udgSettings.getTargetColumn(),
										 udgSettings.getComparator(),
										 udgSettings.getUserVariable());
	}

	//TO-DO: Let user define Holistic Dataset? Or keep it like that?
	public IRowCheck createUserDefinedHolisticCheck(UserDefinedHolisticSettings udhSettings, String targetDatasetAlias)
	{
		Dataset<Row> df = null;
		for (DatasetProfile profile : profiles)
		{
			if (profile.getAlias().equals(targetDatasetAlias))
			{
				df = profile.getDataset();
				break;
			}
		}

		return new UserDefinedRowValueComparisonToAggValueCheck(udhSettings.getTargetColumn(),
											udhSettings.getComparator(),
											udhSettings.getUserVariable(),
											df);
	}
}
