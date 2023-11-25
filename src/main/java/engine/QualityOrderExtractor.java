package engine;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import rowchecks.IRowCheck;
import rowchecks.RowCheckFactory;

import utils.DomainTypeSettings;
import utils.DomainValueSettings;
import utils.ForeignKeySettings;
import utils.NotNullSettings;
import utils.NumberConstraintSettings;

public class QualityOrderExtractor {

	public ArrayList<IRowCheck> getRowChecksFromOrder(QualityOrder order, HashMap<String, Dataset<Row>> registeredSets)
	{
		ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
		RowCheckFactory factory = new RowCheckFactory(registeredSets);

		if (order.getForeignKeyChecks() != null)
		{
			for (ForeignKeySettings fkSettings : order.getForeignKeyChecks())
			{		
				checks.add(factory.createBTreeForeignKeyCheck(fkSettings));
			}
		}

		if (order.getDomainTypeChecks() != null)
		{
			for (DomainTypeSettings dtSettings : order.getDomainTypeChecks())
			{
				checks.add(factory.createDomainTypeCheck(dtSettings));
			}
		}

		if (order.getDomainValueChecks() != null)
		{
			for (DomainValueSettings dvSettings : order.getDomainValueChecks())
			{
				checks.add(factory.createDomainValuesCheck(dvSettings));
			}
		}

		if (order.getNotNullChecks() != null)
		{
			for (NotNullSettings nnSettings : order.getNotNullChecks())
			{
				checks.add(factory.createNotNullCheck(nnSettings));
			}
		}

		if (order.getNumberConstraintChecks() != null)
		{
			for (NumberConstraintSettings ncSettings : order.getNumberConstraintChecks())
			{
				checks.add(factory.createNumericConstraintCheck(ncSettings));
			}
		}

		return checks;
	}
}
