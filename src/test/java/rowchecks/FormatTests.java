package rowchecks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import utils.CheckResult;
import utils.FormatType;

public class FormatTests extends RowCheckTest{
    

    @Test
    public void formatGoodDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        excludedResult = CheckResult.FAILED;
        rowChecks.add(new FormatCheck("date", FormatType.DD_MM_YYYY, "-"));
        rowChecks.add(new FormatCheck("date", FormatType.DD_MM, "-"));
        rowChecks.add(new FormatCheck("date", FormatType.MM_DD, "-"));
        rowChecks.add(new FormatCheck("date", FormatType.MM_DD_YYYY, "-"));
        rowChecks.add(new FormatCheck("date", FormatType.MM_YYYY, "-"));
        rowChecks.add(new FormatCheck("date", FormatType.YYYY_MM, "-"));
        rowChecks.add(new FormatCheck("date", FormatType.YYYY_MM_DD, "-"));

        testSet.foreach(row -> { checkRowWithExclusion(row); });

        assertEquals(595, excludedRows.size());
    }

    @Test
    public void formatBadDayTest()
    {
        rowChecks.clear();
        excludedRows.clear();
        excludedResult = CheckResult.FAILED;
        rowChecks.add(new FormatCheck("date", FormatType.DD_MM_YYYY, "1234"));

        testSet.foreach(row -> { checkRowWithExclusion(row); });

        assertEquals(100, excludedRows.size());
    }
}
