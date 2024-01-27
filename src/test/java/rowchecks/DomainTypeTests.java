package rowchecks;

import org.junit.Test;

import rowchecks.checks.DomainTypeCheck;
import utils.CheckResult;
import utils.DomainType;

public class DomainTypeTests extends RowCheckTest {
    
    @Test
    public void domainTypeGoodDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new DomainTypeCheck("model", DomainType.ALPHA));
        rowChecks.add(new DomainTypeCheck("year", DomainType.INTEGER));
        rowChecks.add(new DomainTypeCheck("mpg", DomainType.NUMERIC));
        rowChecks.add(new DomainTypeCheck("isOldModel", DomainType.BOOLEAN));


        testSet.foreach(row -> { checkRow(row); });
    }

    @Test
    public void domainTypeBadDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.REJECTED;
        rowChecks.add(new DomainTypeCheck("year",DomainType.ALPHA));
        rowChecks.add(new DomainTypeCheck("model",DomainType.INTEGER));
        rowChecks.add(new DomainTypeCheck("model",DomainType.NUMERIC));
        rowChecks.add(new DomainTypeCheck("model",DomainType.BOOLEAN));

        testSet.foreach(row -> { checkRow(row); });
    }
}
