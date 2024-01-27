package rowchecks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import rowchecks.checks.UserDefinedConditionalDependencyCheck;
import utils.CheckResult;

public class UDConditionalDependencyTests extends RowCheckTest  {
    
    @Test
    public void conditionalDependencyGoodDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new UserDefinedConditionalDependencyCheck("tax", ">=", "0",
                                                                "price", ">", "0"));

        testSet.foreach(row -> {checkRow(row);} );
    }

    @Test
    public void conditionalDependencyBadDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        excludedResult = CheckResult.PASSED;
        rowChecks.add(new UserDefinedConditionalDependencyCheck("price", ">=", "10000",
                                                                "engineSize", "<", "2"));

        testSet.foreach(row -> {checkRowWithExclusion(row);} );
        //43 entries have price >= 10k and satisfy the engineSize < 2 condition
        //3 have price < 10k. 46 passed total.
        assertEquals(46, excludedRows.size());
    }

    @Test
    public void conditionalDependencyWorstDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.ILLEGAL_FIELD;
        rowChecks.add(new UserDefinedConditionalDependencyCheck("imaginary", ">=", "10000",
                                                                "1", "<", "2"));
        rowChecks.add(new UserDefinedConditionalDependencyCheck("2", ">=", "1",
                                                                "fictional", "<", "2"));

        //Columns imaginary and fictional don't exist. All fail.
        testSet.foreach(row -> {checkRow(row);} );
    }
}
