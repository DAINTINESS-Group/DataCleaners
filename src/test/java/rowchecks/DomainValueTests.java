package rowchecks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import utils.CheckResult;

public class DomainValueTests extends RowCheckTest {
    
    @Test
    public void domainValuesGoodDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new DomainValuesCheck("zero_to_ten", new String[] {"0","1","2","3","4","5","6","7","8","9","10"}));

        testSet.foreach(row -> { checkRow(row); });

        rowChecks.clear();
        rowChecks.add(new DomainValuesCheck("name", new String[] {"Connor"}));

        excludedResult = CheckResult.FAILED;
        testSet.foreach(row -> { checkRowWithExclusion(row); });
        assertEquals(excludedRows.size(),98);
    }

    @Test
    public void domainValuesBadDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.FAILED;
        rowChecks.add(new DomainValuesCheck("null", new String[] {"null"}));
        testSet.foreach(row -> { checkRow(row); });
    }
}
