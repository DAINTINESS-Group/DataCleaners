package engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import rowchecks.IRowCheck;
import rowchecks.IRowCheck.CheckResult;

public class QualityProfilerFacade {

    //TODO: Move this to separate class
    private final static class ForEachChecker implements ForeachFunction<Row>
    {

        static ArrayList<IRowCheck> rowChecks;
        static int count = 0;
        public ForEachChecker(ArrayList<IRowCheck> rc)
        {
            rowChecks = rc;
        }

        public void call(Row row) throws Exception {
            for (IRowCheck c : rowChecks)
            {
                if (c.check(row) == CheckResult.FAILED)
                {
                    System.out.println(row + " - " + c.getCheckType());
                    ForEachChecker.count += 1;
                    break;
                }
            } 
        }

        public int getFails() { return count; }
        
    }





    final static int DATASET_ALREADY_EXISTS = 0;
    final static int INTERNAL_ERROR = -1;

    final static String DBMS_TYPE_MYSQL = "com.mysql.cj.jdbc.Driver";

    static QualityProfilerFacade facade = new QualityProfilerFacade();
    SparkSession spark;
    HashMap<String, Dataset<Row>> dataSets = new HashMap<String, Dataset<Row>>();

    QualityOrderExtractor orderExtractor = new QualityOrderExtractor();


    private QualityProfilerFacade()
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

    public int loadDatasetFromFile(String path, String frameName, boolean hasHeader)
    {
        try
        {
            if (dataSets.containsKey(frameName)) return DATASET_ALREADY_EXISTS;

            Dataset<Row> df = spark.read().option("header",hasHeader).csv(path);
            dataSets.put(frameName, df);
            return 1;
        }
        catch (Exception e)
        {
            return INTERNAL_ERROR;
        }
        
    }

    //TODO Add all exceptions + move to LoadFile class?
    public int loadDatasetFromDatabase(String username, String password, String databaseType, 
                                        String url, String tableName, String frameName)
    {
        try
        {
            if (dataSets.containsKey(frameName)) return DATASET_ALREADY_EXISTS;

            Properties properties = new Properties();
            properties.setProperty("driver", databaseType);
            properties.setProperty("username", username);
            properties.setProperty("password", password);
            
            Dataset<Row> df = spark.read().jdbc(url, tableName, properties);
            dataSets.put(frameName, df);
            return 1;
        }
        catch (Exception e)
        {
            return INTERNAL_ERROR;
        }
        
    }

    //TODO: Add exceptions
    public void runQualityChecks(QualityOrder order)
    {
        ArrayList<IRowCheck> checks = orderExtractor.getRowChecksFromOrder(order);
        Dataset<Row> targetSet = getDataset(order.getTargetDataset());

        ForEachChecker checker = new ForEachChecker(checks);
        targetSet.foreach(checker);
        System.out.println("Number of failed entries: " + checker.getFails());
    }

    public String[] getDataframeColumns(String frameName) { return dataSets.get(frameName).columns(); }
    public Dataset<Row> getDataset(String frameName) { return dataSets.get(frameName); }
    public String[] getDatasetNames() { return dataSets.keySet().toArray(new String[0]); }
    public int getDatasetsLength() { return dataSets.size(); }

    public static QualityProfilerFacade getInstance() { return facade; }
}
