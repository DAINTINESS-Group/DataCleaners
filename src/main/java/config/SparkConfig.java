package config;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class SparkConfig {
    
    private String appName = "Java Spark SQL";
    private String appKey = "spark.master";
    private String appValue = "local";

    private SparkSession spark;

    public SparkConfig()
    {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        spark = SparkSession
                        .builder()
                        .appName("Java Spark SQL")
                        .config("spark.master", "local")
                        .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
    }

    public SparkSession getSparkSession()
    {
        return spark;
    }

    public String getAppName() {
        return appName;
    }

    public String getAppKey() {
        return appKey;
    }

    public String getAppValue() {
        return appValue;
    }

    
}
