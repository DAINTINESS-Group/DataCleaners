package app;

import engine.FacadeFactory;
import engine.IDataCleanerFacade;
import model.ClientRequest;
import model.ClientRequestResponse;
import utils.Comparator;
import utils.ReportType;
import utils.ViolatingRowPolicy;


public class Client {

    public static void main(String[] args)
    {
        IDataCleanerFacade facade;
        FacadeFactory facadeFactory = new FacadeFactory();

        facade = facadeFactory.createDataCleanerFacade();

        facade.registerDataset("src\\test\\resources\\datasets\\cars_100.csv", "frame1", true);
        facade.registerDataset("src\\test\\resources\\datasets\\cars_100.csv", "frame2", true);
    
        final long startTime = System.currentTimeMillis();
        //TO-DO: Performance issue when using price/mileage. Large numbers..?
        ClientRequest req = ClientRequest.builder()
                            .onDataset("frame1")
                            .withColumnValues("manufacturer", new String[] {"audi","bmw","ford","vauxhall","vw","hyundi"})
                            .withColumnValues("transmission", new String[] {"Manual"})
                            .withColumnValues("fuelType", new String[] {"Diesel"})
                            .withCustomCheck("year", "<", "2016") 
                            .withCustomConditionalCheck("engineSize", ">", "1.5", "tax", Comparator.LESS_EQUAL, "145")
                            .withCustomHollisticCheck("mpg", ">=", "AVG(mpg)")
                            .withViolationPolicy(ViolatingRowPolicy.ISOLATE) 
                            .build();
        
        ClientRequestResponse response = facade.executeClientRequest(req);
        //99 rejections, no invalid, 4 passed
        System.out.println("==========RESPONSE RESULT TO CLIENT============");
        System.out.println("Succesful: " + response.isSuccesful());
        System.out.println("Rejected Rows: " + response.getNumberOfRejectedRows());
        System.out.println("Invalid Rows: " + response.getNumberOfInvalidRows());

        final long endTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (endTime - startTime)/1000d + " second(s)");


        //facade.generateReport("frame1", "C:\\Users\\nikos\\Desktop\\OUT", ReportType.TEXT);
    }



}
