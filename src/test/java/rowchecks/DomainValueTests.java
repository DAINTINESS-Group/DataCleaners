package rowchecks;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Test;

import rowchecks.IRowCheck.CheckResult;

public class DomainValueTests extends RowCheckTest {
    
    @Test
    public void domainValuesGoodDayTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        checks.add(new DomainValuesCheck("zero_to_ten", new String[] {"0","1","2","3","4","5","6","7","8","9","10"}));

        testSet.foreach(new ForEachChecker(checks, CheckResult.PASSED));

        checks.clear();
        checks.add(new DomainValuesCheck("name", new String[] {"Connor"}));

        ForEachCheckerWithExclusion checker = new ForEachCheckerWithExclusion(checks, CheckResult.FAILED);
        testSet.foreach(checker);
        assertEquals(checker.getExclusions().size(),98);
    }

    @Test
    public void domainValuesBadDayTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        checks.add(new DomainValuesCheck("null", new String[] {"null"}));
        testSet.foreach(new ForEachChecker(checks, CheckResult.FAILED));
    }
}
