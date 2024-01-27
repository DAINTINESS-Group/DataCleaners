package rowchecks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import rowchecks.checks.UserDefinedRowValueComparisonToAggValueCheck;
import utils.CheckResult;
import utils.Comparator;

public class UDRowValueToAggValueTests extends RowCheckTest {

    @Test
    public void RowValueToAggValueGoodDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new UserDefinedRowValueComparisonToAggValueCheck("1", "<", "2", testSet));
        rowChecks.add(new UserDefinedRowValueComparisonToAggValueCheck("tax", ">=", "tax-1", testSet));
        rowChecks.add(new UserDefinedRowValueComparisonToAggValueCheck("mileagePlusPrice", Comparator.EQUAL, "price+mileage", testSet));

        testSet.foreach(row -> {checkRow(row);} );

        rowChecks.clear();
        excludedResult = CheckResult.REJECTED;
        rowChecks.add(new UserDefinedRowValueComparisonToAggValueCheck("price", ">=", "AVG(price)", testSet));
        testSet.foreach(row -> {checkRowWithExclusion(row);} );

        assertEquals(50, excludedRows.size());
    }

    @Test
    public void RowValueToAggValueBadDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        excludedResult = CheckResult.PASSED;
        //SUM(tax) = 10780 plus 220 = 11000. 2 Entries have price == 11k
        rowChecks.add(new UserDefinedRowValueComparisonToAggValueCheck("price", Comparator.EQUAL, "SUM(tax)+220", testSet));
        //Average of EngineSize is ~1.7 > 1.5. All fail this check.
        rowChecks.add(new UserDefinedRowValueComparisonToAggValueCheck("1.5", Comparator.GREATER_EQUAL, "AVG(engineSize)", testSet));

        testSet.foreach(row -> {checkRowWithExclusion(row);} );

        assertEquals(2, excludedRows.size());
    }

    @Test
    public void RowValueToAggValueWorstDayTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.ILLEGAL_FIELD;
        rowChecks.add(new UserDefinedRowValueComparisonToAggValueCheck("manufacturer", Comparator.EQUAL, "1", testSet));
        rowChecks.add(new UserDefinedRowValueComparisonToAggValueCheck("1", Comparator.NOT_EQUAL, "1+SUM(nonExistantColumn)", testSet));
        //Manufacturer is non-numeric. nonExistantColumnt does not exist. All 103 columns fail.
        testSet.foreach(row -> {checkRow(row);} );


        rowChecks.clear();
        excludedRows.clear();
        excludedResult = CheckResult.MISSING_VALUE;
        rowChecks.add(new UserDefinedRowValueComparisonToAggValueCheck("chaos", Comparator.GREATER, "AVG(chaos)", testSet));
        //5 Null values.
        testSet.foreach(row -> {checkRowWithExclusion(row);} );
        assertEquals(5, excludedRows.size());

        //49 entries of chaos are numeric, with an average of ~481
        //49 other entries have non-numeric values and produce ILLEGAL FIELDS
        excludedRows.clear();
        excludedResult = CheckResult.ILLEGAL_FIELD;
        testSet.foreach(row -> {checkRowWithExclusion(row);} );
        assertEquals(49, excludedRows.size());
        
    }
}
