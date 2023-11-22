package com.dataquality;

import java.util.ArrayList;

import com.rowchecks.BTreeForeignKeyCheck;
import com.rowchecks.DomainTypeCheck;
import com.rowchecks.DomainValuesCheck;
import com.rowchecks.IRowCheck;
import com.rowchecks.NotNullCheck;
import com.rowchecks.NumericConstraintCheck;
import com.utils.DomainTypeSettings;
import com.utils.DomainValueSettings;
import com.utils.ForeignKeySettings;
import com.utils.NotNullSettings;
import com.utils.NumberConstraintSettings;

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
