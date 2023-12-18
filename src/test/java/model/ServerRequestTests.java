package model;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import config.SparkConfig;
import rowchecks.DomainTypeCheck;
import rowchecks.NotNullCheck;
import rowchecks.NumericConstraintCheck;
import utils.DomainType;
import utils.VioletingRowPolicy;

public class ServerRequestTests {
    
    DatasetProfile targetProfile;

    @Before
    public void setUp()
    {
        SparkSession spark = new SparkConfig().getSparkSession();
        Dataset<Row> df = spark.read().option("header",true).csv("src\\test\\resources\\datasets\\test.csv");
        targetProfile = new DatasetProfile("frame", df, "somepath");
    }

    @Test
    public void executeTest()
    {
        ServerRequest serverRequest = new ServerRequest(VioletingRowPolicy.WARN);
        serverRequest.setProfile(targetProfile);

        serverRequest.addRowCheck(new DomainTypeCheck("name", DomainType.ALPHA));
        serverRequest.addRowCheck(new NumericConstraintCheck("float", -1, 0));
        serverRequest.addRowCheck(new NotNullCheck("boolean"));

        serverRequest.execute();
        ServerRequestResult result = serverRequest.getRequestResult();
        assertEquals(46, result.getRejectedRows());
        assertEquals(46, result.getRowCheckResults().where("c1 = 'FAILED'").count());

        assertEquals(0, result.getInvalidRows());
    }
}
