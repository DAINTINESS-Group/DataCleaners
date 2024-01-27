package rowchecks;

import org.junit.Test;

import rowchecks.checks.FormatCheck;
import utils.CheckResult;
import utils.FormatType;

public class FormatTests extends RowCheckTest{
    

    @Test
    public void formatGoodDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        expectedResult = CheckResult.PASSED;
        rowChecks.add(new FormatCheck("daymonthyear", FormatType.DD_MM_YYYY, "/"));
        rowChecks.add(new FormatCheck("daymonth", FormatType.DD_MM, "/"));
        rowChecks.add(new FormatCheck("monthday", FormatType.MM_DD, "/"));
        rowChecks.add(new FormatCheck("monthdayyear", FormatType.MM_DD_YYYY, "/"));
        rowChecks.add(new FormatCheck("monthyear", FormatType.MM_YYYY, "/"));
        rowChecks.add(new FormatCheck("yearmonth", FormatType.YYYY_MM, "/"));
        rowChecks.add(new FormatCheck("yearmonthday", FormatType.YYYY_MM_DD, "/"));

        formatSet.foreach(row -> { checkRow(row); });
    }

    @Test
    public void formatBadDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        expectedResult = CheckResult.REJECTED;
        rowChecks.add(new FormatCheck("dateOfSale", FormatType.DD_MM_YYYY, "1234"));

        testSet.foreach(row -> { checkRow(row); });
    }
}
