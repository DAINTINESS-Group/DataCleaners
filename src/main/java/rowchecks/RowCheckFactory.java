package rowchecks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import utils.DomainType;

//TODO add defensive code for null parameters
public class RowCheckFactory {
	public IRowCheck createDomainTypeCheck(String targetColumn, DomainType type) {
		return new 	DomainTypeCheck(targetColumn, type);
	}

	public IRowCheck createDomainValuesCheck(String targetColumn, String[] domainValues) {
		return new DomainValuesCheck(targetColumn, domainValues);
	}
	
	public IRowCheck createNotNullCheck(String targetColumn) {
		return new NotNullCheck(targetColumn);
	}
	
	public IRowCheck createNumericConstraintCheck(String targetColumn, double minValue, double maxValue, boolean includeMinValue, boolean includeMaxValue) {
		return new NumericConstraintCheck(targetColumn, minValue, maxValue, includeMinValue, includeMaxValue);
	}
	
	public IRowCheck createBTreeForeignKeyCheck(String targetColumn, Dataset<Row> foreignKeyDataset, String foreignKeyColumn) {
		return new BTreeForeignKeyCheck(targetColumn, foreignKeyDataset, foreignKeyColumn);
	}
}//end class
