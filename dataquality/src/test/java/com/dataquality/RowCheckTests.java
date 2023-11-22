package com.dataquality;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import com.rowchecks.DomainTypeCheck;
import com.rowchecks.DomainValuesCheck;
import com.rowchecks.IRowCheck;
import com.rowchecks.NotNullCheck;
import com.rowchecks.NumericConstraintCheck;
import com.rowchecks.IRowCheck.CheckResult;
import com.utils.DomainType;

public class RowCheckTests {

    private final static class ForEachChecker implements ForeachFunction<Row>
    {

        static ArrayList<IRowCheck> rowChecks;
        static CheckResult expectedResult;
        public ForEachChecker(ArrayList<IRowCheck> rowChecks, CheckResult expectedResult)
        {
            this.rowChecks = rowChecks;
            this.expectedResult = expectedResult;
        }

        public void call(Row row) throws Exception {
            for (IRowCheck c : rowChecks)
            {
                assertSame(expectedResult, c.check(row));
            } 
        } 
    }  

    private final static class ForEachCheckerWithExclusion implements ForeachFunction<Row>
    {

        static ArrayList<IRowCheck> rowChecks;
        static CheckResult excludedResult;
        static ArrayList<Row> excludedRows;
        public ForEachCheckerWithExclusion(ArrayList<IRowCheck> rowChecks, CheckResult excludedResult)
        {
            this.rowChecks = rowChecks;
            this.excludedResult = excludedResult;
            this.excludedRows = new ArrayList<Row>();
        }

        public void call(Row row) throws Exception {
            for (IRowCheck c : rowChecks)
            {
                if (c.check(row) == excludedResult)
                {
                    excludedRows.add(row);
                }
            } 
        } 

        public ArrayList<Row> getExclusions() { return excludedRows; }
    }  

    SparkSession spark;
    Dataset<Row> testSet;

    //TO-DO: Add Illegal and NullPointer tests. Generalized for all
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


        testSet = spark.read().option("header",true).csv("src\\dataset\\test.csv");
    }


    @Test
    public void domainTypeGoodDayTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        checks.add(new DomainTypeCheck("name", DomainType.ALPHA));
        checks.add(new DomainTypeCheck("zero_to_ten", DomainType.INTEGER));
        checks.add(new DomainTypeCheck("float", DomainType.NUMERIC));
        checks.add(new DomainTypeCheck("boolean", DomainType.BOOLEAN));

        testSet.foreach(new ForEachChecker(checks, CheckResult.PASSED));
    }

    @Test
    public void domainTypeBadDayTest()
    {
        ArrayList<IRowCheck> failedTests = new ArrayList<IRowCheck>();
        failedTests.add(new DomainTypeCheck("float",DomainType.ALPHA));
        failedTests.add(new DomainTypeCheck("name",DomainType.INTEGER));
        failedTests.add(new DomainTypeCheck("name",DomainType.NUMERIC));
        failedTests.add(new DomainTypeCheck("name",DomainType.BOOLEAN));

        testSet.foreach(new ForEachChecker(failedTests, CheckResult.FAILED));
    }

    @Test
    public void domainValuesGoodDayTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        checks.add(new DomainValuesCheck("zero_to_ten", new String[] {"0","1","2","3","4","5","6","7","8","9","10"}));

        testSet.foreach(new ForEachChecker(checks, CheckResult.PASSED));

        checks.clear();
        checks.add(new DomainValuesCheck("name", new String[] {"Connor"}));

        ForEachCheckerWithExclusion checker = new ForEachCheckerWithExclusion(checks, CheckResult.FAILED);
        testSet.foreach(checker);
        assertEquals(checker.getExclusions().size(),98);
    }

    @Test
    public void domainValuesBadDayTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        checks.add(new DomainValuesCheck("null", new String[] {"null"}));
        testSet.foreach(new ForEachChecker(checks, CheckResult.FAILED));
    }

    @Test
    public void notNullGoodDayTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        checks.add(new NotNullCheck("name"));
        testSet.foreach(new ForEachChecker(checks, CheckResult.PASSED));

        checks.clear();
        checks.add(new NotNullCheck("null"));
        testSet.foreach(new ForEachChecker(checks, CheckResult.FAILED));
    }

    @Test
    public void numericConstraintGoodDayTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        checks.add(new NumericConstraintCheck("float", -1, 1, false, false));
        
        testSet.foreach(new ForEachChecker(checks, CheckResult.PASSED));

        checks.add(new NumericConstraintCheck("negative_to_positive", -100, 100, false, false));
        ForEachCheckerWithExclusion checker = new ForEachCheckerWithExclusion(checks, CheckResult.FAILED);
        testSet.foreach(checker);
        assertEquals(checker.getExclusions().size(),1);
    }

    @Test
    public void numericConstraintBadDayTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        
        checks.add(new NumericConstraintCheck("name", -1, 1, false, false));
        testSet.foreach(new ForEachChecker(checks, CheckResult.FAILED));
    
        checks.clear();
        checks.add(new NumericConstraintCheck("null", -1, 1, false, false));
        testSet.foreach(new ForEachChecker(checks, CheckResult.MISSING_VALUE));
    }

    @Test
    public void missingColumnTest()
    {
        ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
        checks.add(new DomainTypeCheck("xxx", DomainType.ALPHA));
        checks.add(new DomainValuesCheck("xxx", new String[] {"0","1","2","3","4","5","6","7","8","9","10"}));
        checks.add(new NotNullCheck("xxx"));
        checks.add(new NumericConstraintCheck("xxx", -1, 1, false, false));

        testSet.foreach(new ForEachChecker(checks, CheckResult.ILLEGAL_FIELD));
    }


}
