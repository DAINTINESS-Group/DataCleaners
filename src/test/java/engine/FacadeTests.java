package engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;

import engine.clientRequest.ClientRequest;
import engine.clientRequest.ClientRequestResponse;
import model.DatasetProfile;
import utils.DomainType;

public class FacadeTests {
    
    IDataCleanerFacade facade;

    @Before
    public void setUp()
    {
        FacadeFactory facadeFactory = new FacadeFactory();

        facade = facadeFactory.createDataCleanerFacade();
        facade.registerDataset("src\\test\\resources\\datasets\\cars_100_tests.csv", "frame1", true);
        facade.registerDataset("src\\test\\resources\\datasets\\cars_100_tests.csv", "frame2", true);
        
    }

    @Test
    public void fullFlowTest()
    {
        ClientRequest req = ClientRequest.builder()
                                        .onDataset("frame1")
                                        .withNumericColumn("price", 0, 15000)
                                        .withColumnType("isOldModel", DomainType.BOOLEAN)
                                        .withNumericColumn("mpg", 0, 100)
                                        .build();
        ClientRequestResponse response = facade.executeClientRequest(req);

        assertEquals(59, response.getNumberOfRejectedRows());
        assertEquals(0, response.getNumberOfInvalidRows());
        
        req = ClientRequest.builder()
                            .onDataset("frame1")
                            .withColumnType("boolean123", DomainType.NUMERIC)
                            .build();


        response = facade.executeClientRequest(req);

        assertEquals(0, response.getNumberOfRejectedRows());
        assertEquals(103, response.getNumberOfInvalidRows());
    }

    @Test
    public void profileCreationTest()
    {
        try
        {
            Method method = DataCleanerFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
            assertNotNull((DatasetProfile) method.invoke(facade, "frame1"));
            assertNull((DatasetProfile) method.invoke(facade, "notFrame"));
        }
        catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }


}
