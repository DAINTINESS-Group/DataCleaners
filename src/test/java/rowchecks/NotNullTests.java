package rowchecks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import rowchecks.checks.NotNullCheck;
import utils.CheckResult;

public class NotNullTests extends RowCheckTest{
    
    @Test
    public void notNullGoodDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        excludedResult = CheckResult.REJECTED;
        rowChecks.add(new NotNullCheck("chaos"));

        testSet.foreach(row -> { checkRowWithExclusion(row); });
        //5 Null values, 5 rejections.
        assertEquals(5, excludedRows.size());
    }
}
