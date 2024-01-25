package rowchecks;

import org.junit.Test;

import rowchecks.checks.DomainTypeCheck;
import rowchecks.checks.DomainValuesCheck;
import rowchecks.checks.NotNullCheck;
import rowchecks.checks.NumericConstraintCheck;
import utils.CheckResult;
import utils.DomainType;

public class InvalidContentRowCheckTests extends RowCheckTest {
    
    @Test
    public void missingColumnTest()
    {
        rowChecks.clear();
        expectedResult = CheckResult.ILLEGAL_FIELD;
        rowChecks.add(new DomainTypeCheck("xxx", DomainType.ALPHA));
        rowChecks.add(new DomainValuesCheck("xxx", new String[] {"0","1","2","3","4","5","6","7","8","9","10"}));
        rowChecks.add(new NotNullCheck("xxx"));
        rowChecks.add(new NumericConstraintCheck("xxx", -1, 1, false, false));

        testSet.foreach(row -> { checkRow(row); });
    }
}
