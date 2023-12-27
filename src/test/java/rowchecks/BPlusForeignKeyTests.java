package rowchecks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import utils.CheckResult;

public class BPlusForeignKeyTests extends RowCheckTest {
    
    @Test
    public void foreignKeyGoodDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new BPlusTreeForeignKeyCheck("negative_to_positive", testSet, "negative_to_positive"));

        testSet.foreach(row -> { checkRow(row); });
    }

    @Test
    public void foreignKeyBadDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        excludedResult = CheckResult.FAILED;
        rowChecks.add(new BPlusTreeForeignKeyCheck("negative_to_positive", testSet2, "values"));

        testSet.foreach(row -> { checkRowWithExclusion(row); });
        assertEquals(98, excludedRows.size());
    }
}
