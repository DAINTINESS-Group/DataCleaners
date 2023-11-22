package com.dataquality;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import com.utils.DomainType;




public class Main {

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

        facade.loadDatasetFromFile("dataquality\\src\\dataset\\cars_100.csv", "frame1", true);
       
        QualityOrder order = QualityOrder.builder()
                                .onDataset("frame1")
                                .withNoNullValues("price")
                                .withColumnType("year", DomainType.INTEGER)
                                .withNumericColumn("year", 2015, 2020, true, true)
                                .withNumericColumn("price", 0, 18_000, true, false)
                                .withColumnValues("manufacturer", new String[] {"audi"})
                                .build();
        facade.runQualityChecks(order);

    }


}
