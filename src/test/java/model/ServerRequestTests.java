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
        Dataset<Row> df = spark.read().option("header",true).csv("src\\test\\resources\\datasets\\test.csv");
        targetProfile = new DatasetProfile("frame", df, "somepath", true);
    }

    @Test
    public void executeTest()
    {
        ServerRequest serverRequest = new ServerRequest(ViolatingRowPolicy.WARN);
        serverRequest.setProfile(targetProfile);

        serverRequest.addRowCheck(new DomainTypeCheck("name", DomainType.ALPHA));
        serverRequest.addRowCheck(new NumericConstraintCheck("float", -1, 0));
        serverRequest.addRowCheck(new NotNullCheck("boolean"));

        new ServerRequestExecutor().executeServerRequest(serverRequest);
        ServerRequestResult result = serverRequest.getRequestResult();
        assertEquals(46, result.getRejectedRows());
        assertEquals(46, result.getRowCheckResults().where("c1 = 'REJECTED'").count());

        assertEquals(0, result.getInvalidRows());
    }
}
