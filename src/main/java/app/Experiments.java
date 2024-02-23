package app;

import engine.FacadeFactory;
import engine.IDataCleanerFacade;
import engine.clientRequest.ClientRequest;
import utils.Comparator;
import utils.DomainType;

public class Experiments {
   
    static IDataCleanerFacade facade;
    final static String LINE = "============================================";
    final static String SOFT_LINE = "-------------------------------------------";
    
    public static void main(String[] args)
    {
        initFacade(false);
        registerDatasets(false);
        runNumberOfRowsExperiments(true);
        runNumberOfColumnsExperiments(true);
        runPolutionExperiments(true);
    }

    private static void initFacade(boolean printResults)
    {
        long startTime = System.currentTimeMillis();
        facade = new FacadeFactory().createDataCleanerFacade();
        long endTime = System.currentTimeMillis();
        if (printResults) System.out.println("Facade & Spark Init time: " + (endTime-startTime)/1000d + " seconds");
    }

    private static void registerDatasets(boolean printResults)
    {
        System.out.println(LINE);
        String[] files = new String[] {"dataset10k", "dataset10k", "dataset100k", "dataset1mrows",
                                        "dataset3col","dataset10col", "dataset30col", "dataset100col", "dataset300col", "dataset1000col",
                                        "dataset1percent", "dataset5percent","dataset10percent"};
        String basePath = "src\\test\\resources\\experimentdatasets\\";

        double totalTime = 0;
        boolean warmUp = true;
        for (String file : files)
        {
            long startTime = System.currentTimeMillis();
            facade.registerDataset(basePath + file + ".csv", file, true);
            long endTime = System.currentTimeMillis();

            if (warmUp) { warmUp = false; continue;}

            totalTime += (endTime-startTime)/1000d;
            if (printResults)
            {
                System.out.println("Registration of " + file + ", execution time: " + (endTime-startTime)/1000d + " seconds");
            }
            
        }
        if (printResults) System.out.println("Average time: " + (totalTime/files.length));
        
    }

    private static void runNumberOfRowsExperiments(boolean printResults)
    {
        String[] rowDatasets = new String[] {"dataset10k", "dataset10k", "dataset100k","dataset1mrows"};
        
        boolean warmUp = true;
        System.out.println(LINE);
        for (String df : rowDatasets)
        {
            long startTime = System.nanoTime();
            facade.executeClientRequest(getSimpleRowRequest(df));
            long endTime = System.nanoTime();
            long simpleExecutionTime = endTime-startTime;
            if (warmUp) { warmUp = false; continue;}

            startTime = System.nanoTime();
            facade.executeClientRequest(getComplexRowRequest(df));
            endTime = System.nanoTime();
            long complexExecutionTime = endTime-startTime;         

            startTime = System.nanoTime();
            facade.executeClientRequest(getHeavyRowRequest(df));
            endTime = System.nanoTime();
            long heavyExecutionTime = endTime-startTime;
        
            if (printResults)
            {
                System.out.println("For Dataset: " + df +":");
                System.out.println("Simple Request Execution: " + simpleExecutionTime/1000_000_000d + " seconds.");
                System.out.println("Heavy Request Execution: " + heavyExecutionTime/1000_000_000d + " seconds.");
                System.out.println("Complex Request Execution: " + complexExecutionTime/1000_000_000d + " seconds.");
                System.out.println(SOFT_LINE);
            }
            
        }
        
    }

    private static void runNumberOfColumnsExperiments(boolean printResults)
    {
        System.out.println(LINE);
        String[] rowDatasets = new String[] {"dataset10col","dataset10col", "dataset100col", "dataset1000col"};
        
        boolean warmUp = true;
        for (String df : rowDatasets)
        {
            long startTime = System.currentTimeMillis();
            facade.executeClientRequest(getSimpleColumnRequest(df));
            long endTime = System.currentTimeMillis();
            long simpleExecutionTime = endTime-startTime;
            if (warmUp) { warmUp = false; continue;}

            startTime = System.currentTimeMillis();
            facade.executeClientRequest(getComplexColumnRequest(df));
            endTime = System.currentTimeMillis();
            long complexExecutionTime = endTime-startTime;

            startTime = System.currentTimeMillis();
            facade.executeClientRequest(getHeavyColumnRequest(df));
            endTime = System.currentTimeMillis();
            long heavyExecutionTime = endTime-startTime;

            if (printResults)
            {
                System.out.println("For Dataset: " + df +":");
                System.out.println("Simple Request Execution: " + simpleExecutionTime/1000d + " seconds.");
                System.out.println("Heavy Request Execution: " + heavyExecutionTime/1000d + " seconds.");
                System.out.println("Complex Request Execution: " + complexExecutionTime/1000d + " seconds.");
                System.out.println(SOFT_LINE);
            }
        }
        
    }

    private static void runPolutionExperiments(boolean printResults)
    {
        System.out.println(LINE);
        String[] rowDatasets = new String[] {"dataset1percent", "dataset1percent", "dataset5percent","dataset10percent"};
        
        boolean warmUp = true;
        for (String df : rowDatasets)
        {
            long startTime = System.currentTimeMillis();
            facade.executeClientRequest(getSimplePolutionRequest(df));
            long endTime = System.currentTimeMillis();
            long simpleExecutionTime = endTime-startTime;
            if (warmUp) { warmUp = false; continue;}

            startTime = System.currentTimeMillis();
            facade.executeClientRequest(getHeavyPolutionRequest(df));
            endTime = System.currentTimeMillis();
            long heavyExecutionTime = endTime-startTime;

            startTime = System.currentTimeMillis();
            facade.executeClientRequest(getComplexPolutionRequest(df));
            endTime = System.currentTimeMillis();
            long complexExecutionTime = endTime-startTime;
            if (printResults)
            {
                System.out.println("For Dataset: " + df +":");
                System.out.println("Simple Request Execution: " + simpleExecutionTime/1000d + " seconds.");
                System.out.println("Heavy Request Execution: " + heavyExecutionTime/1000d + " seconds.");
                System.out.println("Complex Request Execution: " + complexExecutionTime/1000d + " seconds.");
                System.out.println(SOFT_LINE);
            }
        }
        
    }

    private static ClientRequest getSimpleRowRequest(String frameName)
    {
        return ClientRequest.builder()
        .onDataset(frameName)
        .withColumnType("age", DomainType.INTEGER)
        .withNoNullValues("uniqueId")
        .withNumericColumn("age", 18, 88).build();
    }

    private static ClientRequest getHeavyRowRequest(String frameName)
    {
        return ClientRequest.builder()
        .onDataset(frameName)
        .withPrimaryKeys("uniqueId")
        .withForeignKeys("uniqueId", "dataset10k", "uniqueId")
        .withCustomCheck("wage", Comparator.EQUAL, "age*10+500")
        .withCustomConditionalCheck("age", ">=", "50", "wage", ">=", "age*10")
        .withCustomHollisticCheck("wage", ">=", "AVG(wage)")
        .build();
    }

    private static ClientRequest getComplexRowRequest(String frameName)
    {
        return ClientRequest.builder()
            .onDataset(frameName)
            .withPrimaryKeys("uniqueId")
            .withCustomHollisticCheck("wage", ">=", "AVG(wage)")
            .withForeignKeys("uniqueId", "dataset10k", "uniqueId")
            .withNumericColumn("wage", 0, 2000)
            .withNoNullValues("wage")
            .withColumnType("name", DomainType.ALPHA).build();
    }


    private static ClientRequest getSimpleColumnRequest(String frameName)
    {
        return ClientRequest.builder()
        .onDataset(frameName)
        .withColumnType("col0", DomainType.INTEGER)
        .withNoNullValues("col1")
        .withNumericColumn("col2", 0, 100).build();
    }

    private static ClientRequest getHeavyColumnRequest(String frameName)
    {
        return ClientRequest.builder()
        .onDataset(frameName)
        .withPrimaryKeys("col1")
        .withForeignKeys("col1", "dataset10col", "col1")
        .withCustomCheck("col0", "<=", "100")
        .withCustomConditionalCheck("col0", ">=", "50", "col2", "<", "50")
        .withCustomHollisticCheck("col0", ">=", "AVG(col0)")
        .build();
    }

    private static ClientRequest getComplexColumnRequest(String frameName)
    {
        return ClientRequest.builder()
            .onDataset(frameName)
            .withPrimaryKeys("col1")
            .withCustomHollisticCheck("col0", ">=", "AVG(col0)")
            .withForeignKeys("col1", "dataset10col", "col1")
            .withNumericColumn("col2", 0, 100)
            .withNoNullValues("col1")
            .withColumnType("col2", DomainType.INTEGER).build();
    }

    private static ClientRequest getSimplePolutionRequest(String frameName)
    {
        return ClientRequest.builder()
        .onDataset(frameName)
        .withColumnType("number", DomainType.INTEGER)
        .withNoNullValues("notNull")
        .withNumericColumn("lowerThan10", 0, 9)
        .build();
    }

    private static ClientRequest getHeavyPolutionRequest(String frameName)
    {
        return ClientRequest.builder()
        .onDataset(frameName)
        .withPrimaryKeys("uniqueId")
        .withCustomCheck("lowerThan10", "<=", "9")
        .withCustomConditionalCheck("lowerThan10", "<=", "9", "number", "==", "1")
        .withCustomHollisticCheck("lowerThan10", ">=", "AVG(lowerThan10)-5")
        .build();
    }

    private static ClientRequest getComplexPolutionRequest(String frameName)
    {
        return ClientRequest.builder()
            .onDataset(frameName)
            .withPrimaryKeys("uniqueId")
            .withCustomHollisticCheck("lowerThan10", ">=", "AVG(lowerThan10)-5")
            .withNumericColumn("lowerThan10", 0, 9)
            .withNoNullValues("notNull")
            .withColumnType("number", DomainType.INTEGER).build();
    }
}