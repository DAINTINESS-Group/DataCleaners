package app;

import engine.FacadeFactory;
import engine.IDataCleanerFacade;
import model.ClientRequest;
import model.ClientRequestResponse;
import utils.DomainType;
import utils.ReportType;
import utils.VioletingRowPolicy;


public class Client {

    public static void main(String[] args)
    {
        IDataCleanerFacade facade;
        FacadeFactory facadeFactory = new FacadeFactory();

        facade = facadeFactory.createDataCleanerFacade();

        facade.registerDataset("src\\test\\resources\\datasets\\cars_100.csv", "frame1", true);
        facade.registerDataset("src\\test\\resources\\datasets\\cars_100.csv", "frame2", true);
        
        ClientRequest req = ClientRequest.builder()
                            .onDataset("frame2")
                            .withColumnType("engineSize", DomainType.NUMERIC)
                            //.withFormat("date", FormatType.MM_YYYY, "-") /* USE THIS ON PROPER DATASET (ex. test) */
                            //.withPrimaryKeys("price") /* This can be used here, but will affect the following commented results */
                            .withForeignKeys("manufacturer", "frame2", "manufacturer")
                            .withNumericColumn("price", 0, 20_000)
                            .withColumnType("price", DomainType.INTEGER)
                            .withColumnValues("manufacturer", new String[] {"audi"})
                            .withNoNullValues("price")
                            .withColumnType("manufacturer", DomainType.ALPHA)
                            .withViolationPolicy(VioletingRowPolicy.ISOLATE) 
                            .build();
        
        
        ClientRequestResponse response = facade.executeClientRequest(req);

        //12 Rejected rows. Makes a log file and 2 isolated files (12 rejected entries - 92 passed entries)
        System.out.println("==========RESPONSE RESULT TO CLIENT============");
        System.out.println("Succesful: " + response.isSuccesful());
        System.out.println("Rejected Rows: " + response.getNumberOfRejectedRows());
        System.out.println("Invalid Rows: " + response.getNumberOfInvalidRows());

        
        req = ClientRequest.builder()
              .onDataset("frame2")
              .withPrimaryKeys("engineSize")
              .withNoNullValues("manufacturer")
              .withNumericColumn("engineSize", 0, 10)
              .withViolationPolicy(VioletingRowPolicy.PURGE)
              .build();
        response = facade.executeClientRequest(req);

        //96 rejected entries. Makes log file and 1 file with the 7 passed entries.
        System.out.println("==========RESPONSE 2 RESULT TO CLIENT============");
        System.out.println("Succesful: " + response.isSuccesful());
        System.out.println("Rejected Rows: " + response.getNumberOfRejectedRows());
        System.out.println("Invalid Rows: " + response.getNumberOfInvalidRows());

        //NOTE: Path should be a directory ^_^
        facade.generateReport("frame2", "C:\\Users\\nikos\\Desktop\\OUT", ReportType.TEXT);
    }



}
