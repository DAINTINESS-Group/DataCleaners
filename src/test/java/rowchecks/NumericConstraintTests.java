package rowchecks;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Test;

import rowchecks.IRowCheck.CheckResult;

public class NumericConstraintTests extends RowCheckTest {
    
    @Test
    public void numericConstraintGoodDayTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        checks.add(new NumericConstraintCheck("float", -1, 1, false, false));
        
        testSet.foreach(new ForEachChecker(checks, CheckResult.PASSED));

        checks.add(new NumericConstraintCheck("negative_to_positive", -100, 100, false, false));
        ForEachCheckerWithExclusion checker = new ForEachCheckerWithExclusion(checks, CheckResult.FAILED);
        testSet.foreach(checker);
        assertEquals(checker.getExclusions().size(),1);
    }

    @Test
    public void numericConstraintBadDayTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        
        checks.add(new NumericConstraintCheck("name", -1, 1, false, false));
        testSet.foreach(new ForEachChecker(checks, CheckResult.FAILED));
    
        checks.clear();
        checks.add(new NumericConstraintCheck("null", -1, 1, false, false));
        testSet.foreach(new ForEachChecker(checks, CheckResult.MISSING_VALUE));
    }
}
