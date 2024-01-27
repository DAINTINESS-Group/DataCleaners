package rowchecks;

import static org.junit.Assert.assertSame;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;

import config.SparkConfig;
import rowchecks.api.IRowCheck;
import utils.CheckResult;

public abstract class RowCheckTest {
    SparkSession spark;
    protected static Dataset<Row> testSet;
    protected static Dataset<Row> testSet2;
    protected static Dataset<Row> formatSet;

    static ArrayList<IRowCheck> rowChecks = new ArrayList<IRowCheck>();
    static ArrayList<Row> excludedRows = new ArrayList<Row>();;
    static CheckResult expectedResult;
    static CheckResult excludedResult;

    @Before
    public void setUp()
    {
        spark = new SparkConfig().getSparkSession();
        testSet = spark.read().option("header",true).csv("src\\test\\resources\\datasets\\cars_100_tests.csv");
        testSet2 = spark.read().option("header",true).csv("src\\test\\resources\\datasets\\cars_100_tests.csv");
        formatSet = spark.read().option("header",true).csv("src\\test\\resources\\datasets\\formatTests.csv");

    }

    public static void checkRow(Row row)
    {
        for (IRowCheck c : rowChecks)
        {
            assertSame(expectedResult, c.check(row));
        } 
    }

    public static void checkRowWithExclusion(Row row)
    {
        for (IRowCheck c : rowChecks)
        {
            if (c.check(row) == excludedResult)
            {
                excludedRows.add(row);
            }
        } 
    }
}
