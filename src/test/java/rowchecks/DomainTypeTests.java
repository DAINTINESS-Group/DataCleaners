package rowchecks;

import org.junit.Test;

import utils.CheckResult;
import utils.DomainType;

public class DomainTypeTests extends RowCheckTest {
    
    @Test
    public void domainTypeGoodDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new DomainTypeCheck("name", DomainType.ALPHA));
        rowChecks.add(new DomainTypeCheck("zero_to_ten", DomainType.INTEGER));
        rowChecks.add(new DomainTypeCheck("float", DomainType.NUMERIC));
        rowChecks.add(new DomainTypeCheck("boolean", DomainType.BOOLEAN));


        testSet.foreach(row -> { checkRow(row); });
    }

    @Test
    public void domainTypeBadDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.REJECTED;
        rowChecks.add(new DomainTypeCheck("float",DomainType.ALPHA));
        rowChecks.add(new DomainTypeCheck("name",DomainType.INTEGER));
        rowChecks.add(new DomainTypeCheck("name",DomainType.NUMERIC));
        rowChecks.add(new DomainTypeCheck("name",DomainType.BOOLEAN));

        testSet.foreach(row -> { checkRow(row); });
    }
}
