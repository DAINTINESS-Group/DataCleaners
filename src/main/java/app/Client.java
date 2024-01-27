package app;

import engine.FacadeFactory;
import engine.IDataCleanerFacade;
import engine.clientRequest.ClientRequest;
import engine.clientRequest.ClientRequestResponse;
import utils.Comparator;
import utils.DomainType;
import utils.ReportType;
import utils.ViolatingRowPolicy;


public class Client {

    public static void main(String[] args)
    {
        IDataCleanerFacade facade;
        FacadeFactory facadeFactory = new FacadeFactory();

        facade = facadeFactory.createDataCleanerFacade();

        facade.registerDataset("src\\test\\resources\\datasets\\cars_100k.csv", "frame1", true);
        facade.registerDataset("src\\test\\resources\\datasets\\cars_100.csv", "frame2", true);
    
        final long startTime = System.currentTimeMillis();
        //TO-DO: Performance issue when using price/mileage. Large numbers..?
        ClientRequest req = ClientRequest.builder()
                            .onDataset("frame1")
                            .withColumnType("price", DomainType.INTEGER)
                            .withColumnValues("manufacturer", new String[] {"audi", "vauxhall"})
                            .withForeignKeys("manufacturer", "frame2", "manufacturer")
                            .withCustomHollisticCheck("price", Comparator.GREATER_EQUAL, "AVG(price)")
                            .withCustomConditionalCheck("tax", ">", "1", "price", "<=", "price*tax")
                            .withNumericColumn("engineSize", 0, 3, true, true)
                            .withViolationPolicy(ViolatingRowPolicy.ISOLATE) 
                            .build();
        
        ClientRequestResponse response = facade.executeClientRequest(req);
        //101533 rejected entries
        //9353 Invalid due to missing engineSize
        System.out.println("==========RESPONSE RESULT TO CLIENT============");
        System.out.println("Succesful: " + response.isSuccesful());
        System.out.println("Rejected Rows: " + response.getNumberOfRejectedRows());
        System.out.println("Invalid Rows: " + response.getNumberOfInvalidRows());

        final long endTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (endTime - startTime)/1000d + " second(s)");


        facade.generateReport("frame1", "src\\test\\resources\\reports\\ClientReport", ReportType.TEXT);
    }



}
