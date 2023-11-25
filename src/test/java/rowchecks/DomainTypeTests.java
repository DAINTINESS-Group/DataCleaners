package rowchecks;

import java.util.ArrayList;

import org.junit.Test;

import rowchecks.IRowCheck.CheckResult;
import utils.DomainType;

public class DomainTypeTests extends RowCheckTest{
    
    @Test
    public void domainTypeGoodDayTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        checks.add(new DomainTypeCheck("name", DomainType.ALPHA));
        checks.add(new DomainTypeCheck("zero_to_ten", DomainType.INTEGER));
        checks.add(new DomainTypeCheck("float", DomainType.NUMERIC));
        checks.add(new DomainTypeCheck("boolean", DomainType.BOOLEAN));

        testSet.foreach(new ForEachChecker(checks, CheckResult.PASSED));
    }

    @Test
    public void domainTypeBadDayTest()
    {
        ArrayList<IRowCheck> failedTests = new ArrayList<IRowCheck>();
        failedTests.add(new DomainTypeCheck("float",DomainType.ALPHA));
        failedTests.add(new DomainTypeCheck("name",DomainType.INTEGER));
        failedTests.add(new DomainTypeCheck("name",DomainType.NUMERIC));
        failedTests.add(new DomainTypeCheck("name",DomainType.BOOLEAN));

        testSet.foreach(new ForEachChecker(failedTests, CheckResult.FAILED));
    }
}
