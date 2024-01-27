package rowchecks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import rowchecks.checks.BPlusTreeForeignKeyCheck;
import utils.CheckResult;

public class BPlusForeignKeyTests extends RowCheckTest {
    
    @Test
    public void foreignKeyGoodDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new BPlusTreeForeignKeyCheck("price", testSet, "price"));

        testSet.foreach(row -> { checkRow(row); });
    }

    @Test
    public void foreignKeyBadDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        excludedResult = CheckResult.REJECTED;
        rowChecks.add(new BPlusTreeForeignKeyCheck("taxedPrice", testSet2, "price"));

        testSet.foreach(row -> { checkRowWithExclusion(row); });
        //13 instances of taxedPrices exist in the price column. So 90 exclusions
        assertEquals(90, excludedRows.size());
    }
}
