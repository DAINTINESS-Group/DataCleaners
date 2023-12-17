package app;

import engine.FacadeFactory;
import engine.IDataCleanerFacade;
import model.ClientRequest;
import model.ClientRequestResponse;
import utils.DomainType;
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
                            .onDataset("frame1")
                            .withForeignKeys("manufacturer", "frame2", "manufacturer")
                            .withNumericColumn("price", 0, 20_000)
                            .withColumnType("price", DomainType.INTEGER)
                            .withColumnValues("manufacturer", new String[] {"audi"})
                            .withNoNullValues("price")
                            .withColumnType("manufacturer", DomainType.ALPHA)
                            .withViolationPolicy(VioletingRowPolicy.ISOLATE)
                            .build();
        
        
        ClientRequestResponse response = facade.executeClientRequest(req);

        System.out.println("==========RESPONSE RESULT TO CLIENT============");
        System.out.println("Succesful: " + response.isSuccesful());
        System.out.println("Rejected Rows: " + response.getNumberOfRejectedRows());
        System.out.println("Invalid Rows: " + response.getNumberOfInvalidRows());
  
    }



}
