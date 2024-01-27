package model;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import config.SparkConfig;
import engine.ServerRequestExecutor;
import rowchecks.checks.DomainTypeCheck;
import rowchecks.checks.NotNullCheck;
import rowchecks.checks.NumericConstraintCheck;
import utils.DomainType;
import utils.ViolatingRowPolicy;

public class ServerRequestTests {
    
    DatasetProfile targetProfile;

    @Before
    public void setUp()
    {
        SparkSession spark = new SparkConfig().getSparkSession();
        Dataset<Row> df = spark.read().option("header",true).csv("src\\test\\resources\\datasets\\cars_100_tests.csv");
        targetProfile = new DatasetProfile("frame", df, "somepath", true);
    }

    @Test
    public void executeTest()
    {
        ServerRequest serverRequest = new ServerRequest(ViolatingRowPolicy.WARN);
        serverRequest.setProfile(targetProfile);

        serverRequest.addRowCheck(new DomainTypeCheck("manufacturer", DomainType.ALPHA));
        serverRequest.addRowCheck(new NumericConstraintCheck("tax", 0, 300));
        serverRequest.addRowCheck(new NotNullCheck("chaos"));

        new ServerRequestExecutor().executeServerRequest(serverRequest);
        ServerRequestResult result = serverRequest.getRequestResult();
        assertEquals(5, result.getRejectedRows());
        assertEquals(5, result.getRowCheckResults().where("c2 = 'REJECTED'").count());

        assertEquals(0, result.getInvalidRows());
    }
}
