package rowchecks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import utils.CheckResult;

public class NumericConstraintTests extends RowCheckTest {
    
    @Test
    public void numericConstraintGoodDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new NumericConstraintCheck("float", -1, 1, false, false));
        
        testSet.foreach(row -> { checkRow(row); });

        excludedRows.clear();
        excludedResult = CheckResult.FAILED;
        rowChecks.add(new NumericConstraintCheck("negative_to_positive", -100, 100, false, false));
        testSet.foreach(row -> { checkRowWithExclusion(row); });
        assertEquals(1,excludedRows.size());
    }

    @Test
    public void numericConstraintBadDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.FAILED;
        
        rowChecks.add(new NumericConstraintCheck("name", -1, 1, false, false));
        testSet.foreach(row -> { checkRow(row);});
    
        rowChecks.clear();
        expectedResult = CheckResult.MISSING_VALUE;
        rowChecks.add(new NumericConstraintCheck("null", -1, 1, false, false));
        testSet.foreach(row -> { checkRow(row);});
    }
}
