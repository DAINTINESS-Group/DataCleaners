package rowchecks;

import org.junit.Test;

import utils.CheckResult;

public class NotNullTests extends RowCheckTest{
    
    @Test
    public void notNullGoodDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new NotNullCheck("name"));
        testSet.foreach(row -> { checkRow(row); });

        rowChecks.clear();
        expectedResult = CheckResult.FAILED;
        rowChecks.add(new NotNullCheck("null"));
        testSet.foreach(row -> { checkRow(row); });
    }
}
