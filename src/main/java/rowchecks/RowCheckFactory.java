package rowchecks;

import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import utils.DomainTypeSettings;
import utils.DomainValueSettings;
import utils.ForeignKeySettings;
import utils.NotNullSettings;
import utils.NumberConstraintSettings;

//TODO add defensive code for null parameters
public class RowCheckFactory {

	HashMap<String, Dataset<Row>> registeredSets;
	public RowCheckFactory(HashMap<String, Dataset<Row>> registeredSets)
	{
		this.registeredSets = registeredSets;
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
		return new BTreeForeignKeyCheck(fkSettings.getTargetColumn(), 
										registeredSets.get(fkSettings.getForeignKeyDataset()),
										fkSettings.getForeignKeyColumn());
	}
}
