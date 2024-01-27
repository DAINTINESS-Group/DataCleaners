package rowchecks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import rowchecks.checks.BPlusTreePrimaryKeyCheck;
import utils.CheckResult;

public class BPlusPrimaryKeyTests extends RowCheckTest {
    
    @Test
    public void primaryKeyGoodDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new BPlusTreePrimaryKeyCheck("uniqueId"));

        testSet.foreach(row -> { checkRow(row);});
    }

    @Test
    public void primaryKeyBadDayTest()
    {
        rowChecks.clear();
        excludedResult = CheckResult.REJECTED;
        rowChecks.add(new BPlusTreePrimaryKeyCheck("price"));

        testSet.foreach(row -> { checkRowWithExclusion(row);});
        //64 Unique values in column price. 39 Rejections.
        assertEquals(39, excludedRows.size());
    }

    @Test
    public void multiplePKChecksTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new BPlusTreePrimaryKeyCheck("uniqueId"));
        rowChecks.add(new BPlusTreePrimaryKeyCheck("uniqueId"));

        testSet.foreach(row -> { checkRow(row);});
    }
}
