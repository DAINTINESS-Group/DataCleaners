package app;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import engine.QualityOrder;
import engine.QualityProfilerFacade;
import utils.DomainType;




public class MainApp {

    static SparkSession spark;
    static QualityProfilerFacade facade = QualityProfilerFacade.getInstance();

    public static void main(String[] args)
    {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        spark = SparkSession
                        .builder()
                        .appName("Java Spark SQL")
                        .config("spark.master", "local")
                        .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        facade.loadDatasetFromFile("src\\test\\resources\\datasets\\cars_100.csv", "frame1", true);
        facade.loadDatasetFromFile("src\\test\\resources\\datasets\\cars_100.csv", "frame2", true);
       
        QualityOrder order = QualityOrder.builder()
                                .onDataset("frame1")
                                .withForeignKeys("price","frame2","price")
                                .withColumnType("price", DomainType.INTEGER)
                                .withColumnValues("manufacturer", new String[] {"audi"})
                                .withNoNullValues("engineSize")
                                .withNumericColumn("engineSize", 0, 2)
                                .build();
        facade.runQualityChecks(order);

    }


}
