package engine;

import java.util.ArrayList;

 import rowchecks.BTreeForeignKeyCheck;
 import rowchecks.DomainTypeCheck;
 import rowchecks.DomainValuesCheck;
 import rowchecks.IRowCheck;
 import rowchecks.NotNullCheck;
 import rowchecks.NumericConstraintCheck;
 import utils.DomainTypeSettings;
 import utils.DomainValueSettings;
 import utils.ForeignKeySettings;
 import utils.NotNullSettings;
 import utils.NumberConstraintSettings;

//TODO: Make constructors accepts Settings classes. 
//TODO: Find a way to make this not look like a mess
public class QualityOrderExtractor {
    
    QualityProfilerFacade facade = QualityProfilerFacade.getInstance();

    public ArrayList<IRowCheck> getRowChecksFromOrder(QualityOrder order)
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();

        if (order.getForeignKeyChecks() != null)
        {
            for (ForeignKeySettings fkSettings : order.getForeignKeyChecks())
            {
                checks.add(new BTreeForeignKeyCheck(fkSettings.getTargetColumn(),
                                                    facade.getDataset(fkSettings.getForeignKeyDataset()),
                                                    fkSettings.getForeignKeyColumn()));
            }
        }
        
        if (order.getDomainTypeChecks() != null)
        {
            for (DomainTypeSettings dtSettings : order.getDomainTypeChecks())
            {
                checks.add(new DomainTypeCheck(dtSettings.getTargetColumn(), dtSettings.getType()));
            }
        }

        if (order.getDomainValueChecks() != null)
        {
            for (DomainValueSettings dvSettings : order.getDomainValueChecks())
            {
                checks.add(new DomainValuesCheck(dvSettings.getTargetColumn(), dvSettings.getValues()));
            }
        }

        if (order.getNotNullChecks() != null)
        {
            for (NotNullSettings nnSettings : order.getNotNullChecks())
            {
                checks.add(new NotNullCheck(nnSettings.getTargetColumn()));
            }
        }

        if (order.getNumberConstraintChecks() != null)
        {
            for (NumberConstraintSettings ncSettings : order.getNumberConstraintChecks())
            {
                checks.add(new NumericConstraintCheck(ncSettings.getTargetColumn(), ncSettings.getMinValue(),
                    ncSettings.getMaxValue(), ncSettings.isIncludeMinimum(), ncSettings.isIncludeMaximum()));
            }
        }

        return checks;
    }
}
