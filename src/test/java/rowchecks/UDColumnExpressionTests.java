package rowchecks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import rowchecks.checks.UserDefinedColumnExpressionRowCheck;
import utils.CheckResult;
import utils.Comparator;

public class UDColumnExpressionTests extends RowCheckTest {

    @Test
    public void columnExpressionGoodDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new UserDefinedColumnExpressionRowCheck("1", "<", "2"));
        rowChecks.add(new UserDefinedColumnExpressionRowCheck("tax", ">=", "tax-1"));
        rowChecks.add(new UserDefinedColumnExpressionRowCheck("mileagePlusPrice", Comparator.EQUAL, "price+mileage"));

        testSet.foreach(row -> {checkRow(row);} );
    }

    @Test
    public void columnExpressionBadDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        excludedResult = CheckResult.PASSED;
        rowChecks.add(new UserDefinedColumnExpressionRowCheck("price", Comparator.EQUAL, "taxedPrice"));
        rowChecks.add(new UserDefinedColumnExpressionRowCheck("tax", Comparator.EQUAL, "150"));

        testSet.foreach(row -> {checkRowWithExclusion(row);} );
        //TaxedPrice = price*(tax/150). So we count 7 cases where tax == 150 and thus price == TaxedPrice
        //We expect 7 passed entries accross 2 checks, so 14 exclusions/
        assertEquals(14, excludedRows.size());
    }

    @Test
    public void columnExpressionWorstDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.ILLEGAL_FIELD;
        rowChecks.add(new UserDefinedColumnExpressionRowCheck("manufacturer", Comparator.EQUAL, "1"));
        rowChecks.add(new UserDefinedColumnExpressionRowCheck("tax", Comparator.NOT_EQUAL, "1+nonExistantColumn"));
        //Manufacturer is non-numeric. nonExistantColumnt does not exist. All 103 columns fail.
        testSet.foreach(row -> {checkRow(row);} );


        rowChecks.clear();
        excludedRows.clear();
        excludedResult = CheckResult.MISSING_VALUE;
        rowChecks.add(new UserDefinedColumnExpressionRowCheck("chaos", Comparator.GREATER, "-1"));
        //5 Null values.
        testSet.foreach(row -> {checkRowWithExclusion(row);} );
        assertEquals(5, excludedRows.size());
        
    }
    
}
