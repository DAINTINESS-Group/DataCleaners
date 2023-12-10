package model.rowcheckresults;

import utils.DomainType;

public class RowResultFactory {
    
    public IRowCheckResult createDomainTypeCheckResult(String targetColumn, DomainType type)
    {
        return new DomainTypeCheckResult(targetColumn, type);
    }

    public IRowCheckResult createDomainValuesCheckResult(String targetColumn)
    {
        return new DomainValuesCheckResult(targetColumn);
    }

    public IRowCheckResult createForeignKeyCheckResult(String targetColumn, String foreignKeyColumn)
    {
        return new ForeignKeyCheckResult(targetColumn, foreignKeyColumn);
    }

    public IRowCheckResult createNotNullCheckResult(String targetColumn)
    {
        return new NotNullCheckResult(targetColumn);
    }

    public IRowCheckResult createNumericConstraintCheckResult(String targetColumn, double minValue, double maxValue,
                                                                boolean isLeftInclusive, boolean isRightInclusive)
    {
        return new NumericConstraintCheckResult(targetColumn, minValue, maxValue, isLeftInclusive, isRightInclusive);
    }
}
