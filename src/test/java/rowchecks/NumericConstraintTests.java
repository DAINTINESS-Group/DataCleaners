package rowchecks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import rowchecks.checks.NumericConstraintCheck;
import utils.CheckResult;

public class NumericConstraintTests extends RowCheckTest {
    
    @Test
    public void numericConstraintGoodDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new NumericConstraintCheck("tax", 0, 205, true, true));
        
        testSet.foreach(row -> { checkRow(row); });

        excludedRows.clear();
        excludedResult = CheckResult.REJECTED;
        rowChecks.add(new NumericConstraintCheck("year", 2013, 2019, false, false));
        testSet.foreach(row -> { checkRowWithExclusion(row); });
        //9 years equal to 2019, 2 equal to 2013. 11 rejections total
        assertEquals(11,excludedRows.size());
    }

    @Test
    public void numericConstraintBadDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.REJECTED;
        
        rowChecks.add(new NumericConstraintCheck("model", -1, 1, false, false));
        testSet.foreach(row -> { checkRow(row);});
    
        rowChecks.clear();
        excludedRows.clear();
        excludedResult = CheckResult.MISSING_VALUE;
        rowChecks.add(new NumericConstraintCheck("chaos", 0, 1000, true, true));
        testSet.foreach(row -> { checkRowWithExclusion(row);});
        //5 missing values.
        assertEquals(5,excludedRows.size());

        excludedRows.clear();
        excludedResult = CheckResult.PASSED;
        testSet.foreach(row -> { checkRowWithExclusion(row);});
        //49 numeric values within [0,1000]
        assertEquals(49,excludedRows.size());


    }
}
