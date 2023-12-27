package rowchecks;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import model.DatasetProfile;
import utils.settings.DomainTypeSettings;
import utils.settings.DomainValueSettings;
import utils.settings.ForeignKeySettings;
import utils.settings.FormatSettings;
import utils.settings.NotNullSettings;
import utils.settings.NumberConstraintSettings;
import utils.settings.PrimaryKeySettings;

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
}
