package rowchecks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import utils.CheckResult;

public class BPlusPrimaryKeyTests extends RowCheckTest {
    
    @Test
    public void primaryKeyGoodDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new BPlusTreePrimaryKeyCheck("float"));

        testSet.foreach(row -> { checkRow(row);});
    }

    @Test
    public void primaryKeyBadDayTest()
    {
        rowChecks.clear();
        excludedResult = CheckResult.FAILED;
        rowChecks.add(new BPlusTreePrimaryKeyCheck("name"));

        testSet.foreach(row -> { checkRowWithExclusion(row);});

        assertEquals(5, excludedRows.size());
    }

    @Test
    public void multiplePKChecksTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new BPlusTreePrimaryKeyCheck("float"));
        rowChecks.add(new BPlusTreePrimaryKeyCheck("float"));

        testSet.foreach(row -> { checkRow(row);});
    }
}
