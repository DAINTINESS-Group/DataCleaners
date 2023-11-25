package rowchecks;

import static org.junit.Assert.assertSame;

import java.util.ArrayList;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;

import rowchecks.IRowCheck.CheckResult;

class ForEachChecker implements ForeachFunction<Row>
{
    static ArrayList<IRowCheck> rowChecks;
    static CheckResult expectedResult;
    
    public ForEachChecker(ArrayList<IRowCheck> rowChecks, CheckResult expectedResult)
    {
        ForEachChecker.rowChecks = rowChecks;
        ForEachChecker.expectedResult = expectedResult;
    }

    public void call(Row row) throws Exception {
        for (IRowCheck c : rowChecks)
        {
            assertSame(expectedResult, c.check(row));
        } 
    } 
}  