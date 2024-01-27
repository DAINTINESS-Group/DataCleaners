package rowchecks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import rowchecks.checks.DomainValuesCheck;
import utils.CheckResult;

public class DomainValueTests extends RowCheckTest {
    
    @Test
    public void domainValuesGoodDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new DomainValuesCheck("manufacturer", new String[] {"audi"}));

        testSet.foreach(row -> { checkRow(row); });

        rowChecks.clear();
        rowChecks.add(new DomainValuesCheck("transmission", new String[] {"Automatic","Manual"}));

        excludedResult = CheckResult.REJECTED;
        testSet.foreach(row -> { checkRowWithExclusion(row); });
        //4 Semi-Autos -> 4 rejections
        assertEquals(4, excludedRows.size());
    }

    @Test
    public void domainValuesBadDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.REJECTED;
        rowChecks.add(new DomainValuesCheck("fuelType", new String[] {"Asteroskoni"}));
        testSet.foreach(row -> { checkRow(row); });
    }
}
