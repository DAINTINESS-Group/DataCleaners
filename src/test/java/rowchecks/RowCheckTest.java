package rowchecks;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;

public abstract class RowCheckTest {
    SparkSession spark;
    Dataset<Row> testSet;

    @Before
    public void setUp()
    {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        spark = SparkSession
                        .builder()
                        .appName("Java Spark SQL")
                        .config("spark.master", "local")
                        .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");


        testSet = spark.read().option("header",true).csv("src\\test\\resources\\datasets\\test.csv");
    }
}
