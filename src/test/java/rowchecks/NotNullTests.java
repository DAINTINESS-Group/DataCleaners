package rowchecks;

import java.util.ArrayList;

import org.junit.Test;

import rowchecks.IRowCheck.CheckResult;

public class NotNullTests extends RowCheckTest{
    
    @Test
    public void notNullGoodDayTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        checks.add(new NotNullCheck("name"));
        testSet.foreach(new ForEachChecker(checks, CheckResult.PASSED));

        checks.clear();
        checks.add(new NotNullCheck("null"));
        testSet.foreach(new ForEachChecker(checks, CheckResult.FAILED));
    }
}
