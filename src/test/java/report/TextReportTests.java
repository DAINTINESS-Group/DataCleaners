package report;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.junit.Before;
import org.junit.Test;

import config.SparkConfig;
import engine.ServerRequestExecutor;
import model.DatasetProfile;
import model.ServerRequest;
import rowchecks.RowCheckTest;
import rowchecks.api.IRowCheck;
import rowchecks.checks.NumericConstraintCheck;
import utils.ViolatingRowPolicy;

public class TextReportTests extends RowCheckTest{
    
    DatasetProfile targetProfile;
    String path = "src\\test\\report-output";

    @Before
    public void setUp()
    {
        SparkSession spark = new SparkConfig().getSparkSession();
        Dataset<Row> df = spark.read().option("header",true).csv("src\\test\\resources\\datasets\\test.csv")
                        .withColumn("_id", functions.row_number().over(Window.orderBy(functions.lit("A"))));;
        targetProfile = new DatasetProfile("frame", df, "somepath", true);
    }

    @Test
    public void warnReportGoodDayTest()
    {        
        initGoodDayTest(ViolatingRowPolicy.WARN);
        File outputFile = new File(path + "\\frame-request0-log.txt");
        File expectedFile = new File("src\\test\\resources\\reports\\goodDayWarn.txt");
        try
        {
            assertTrue(outputFile.exists());
            assertTrue(areFilesSimilar(expectedFile, outputFile));
        }
        catch (Exception e)
        {
            assertTrue(false);
        }
        
    }

    @Test
    public void isolateReportGoodDayTest()
    {        
        initGoodDayTest(ViolatingRowPolicy.ISOLATE);
        File logFile = new File(path + "\\frame-request0-log.txt");
        File passedEntries = new File(path + "\\frame-request0-passedEntries.csv");
        File rejectedEntries = new File(path + "\\frame-request0-rejectedEntries.csv");

        File expectedLog = new File("src\\test\\resources\\reports\\goodDayWarn.txt");
        File expectedPassed = new File("src\\test\\resources\\reports\\goodDayPassed.csv");
        File expectedRejected = new File("src\\test\\resources\\reports\\goodDayRejected.csv");
        try
        {
            assertTrue(logFile.exists());
            assertTrue(passedEntries.exists());
            assertTrue(rejectedEntries.exists());

            assertTrue(areFilesSimilar(logFile, expectedLog));
            assertTrue(areFilesSimilar(passedEntries, expectedPassed));
            assertTrue(areFilesSimilar(rejectedEntries, expectedRejected));
        }
        catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
        
    }


    @Test
    public void purgeReportGoodDayTest()
    {        
        initGoodDayTest(ViolatingRowPolicy.PURGE);
        File logFile = new File(path + "\\frame-request0-log.txt");
        File passedEntries = new File(path + "\\frame-request0-passedEntries.csv");

        File expectedLog = new File("src\\test\\resources\\reports\\goodDayWarn.txt");
        File expectedPassed = new File("src\\test\\resources\\reports\\goodDayPassed.csv");
        try
        {
            assertTrue(logFile.exists());
            assertTrue(passedEntries.exists());

            assertTrue(areFilesSimilar(logFile, expectedLog));
            assertTrue(areFilesSimilar(passedEntries, expectedPassed));
        }
        catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
        
    }

    private boolean areFilesSimilar(File f1, File f2) throws Exception
    {
        List<String> lines1 = FileUtils.readLines(f1, "utf-8");
        ArrayList<String> cleanLines1 = new ArrayList<String>();
        for (String s : lines1)
        {
            String temp = s.trim();
            if (temp.equals("")) continue;
            cleanLines1.add(temp);
        }


        List<String> lines2 = FileUtils.readLines(f2, "utf-8");
        for (String s : lines2)
        {
            String temp = s.trim();
            if (temp.equals("")) continue;
            if (!cleanLines1.remove(temp)) return false;
        }
        if (cleanLines1.size() > 0) return false;
        return true;
        
    }

    private void initGoodDayTest(ViolatingRowPolicy policy)
    {
        ServerRequest serverReq = new ServerRequest(policy);
        
        ArrayList<IRowCheck> rowChecks = new ArrayList<IRowCheck>();
        rowChecks.add(new NumericConstraintCheck("zero_to_ten", 0, 9));
        serverReq.setRowChecks(rowChecks);

        serverReq.setProfile(targetProfile);
        targetProfile.addServerRequest(serverReq);

        new ServerRequestExecutor().executeServerRequest(serverReq);
        
        IReportGenerator reportGen = new StaticTxtReportGenerator();
        reportGen.generateReport(targetProfile, path);
    }
}
