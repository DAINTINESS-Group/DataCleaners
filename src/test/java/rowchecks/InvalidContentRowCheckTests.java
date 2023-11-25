package rowchecks;

import java.util.ArrayList;

import org.junit.Test;

import rowchecks.IRowCheck.CheckResult;
import utils.DomainType;

public class InvalidContentRowCheckTests extends RowCheckTest {
    
    @Test
    public void missingColumnTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        checks.add(new DomainTypeCheck("xxx", DomainType.ALPHA));
        checks.add(new DomainValuesCheck("xxx", new String[] {"0","1","2","3","4","5","6","7","8","9","10"}));
        checks.add(new NotNullCheck("xxx"));
        checks.add(new NumericConstraintCheck("xxx", -1, 1, false, false));

        testSet.foreach(new ForEachChecker(checks, CheckResult.ILLEGAL_FIELD));
    }
}
