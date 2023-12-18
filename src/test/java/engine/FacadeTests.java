package engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;

import model.ClientRequest;
import model.ClientRequestResponse;
import model.DatasetProfile;
import utils.DomainType;

public class FacadeTests {
    
    IDataCleanerFacade facade;

    @Before
    public void setUp()
    {
        FacadeFactory facadeFactory = new FacadeFactory();

        facade = facadeFactory.createDataCleanerFacade();
        facade.registerDataset("src\\test\\resources\\datasets\\test.csv", "frame1", true);
        facade.registerDataset("src\\test\\resources\\datasets\\test.csv", "frame2", true);
        
    }

    @Test
    public void fullFlowTest()
    {
        ClientRequest req = ClientRequest.builder()
                                        .onDataset("frame1")
                                        .withNumericColumn("zero_to_ten", 0, 10)
                                        .withColumnType("boolean", DomainType.BOOLEAN)
                                        .withNumericColumn("float", 0, 1)
                                        .build();
        System.out.println(req.getNumberConstraintChecks().size());
        ClientRequestResponse response = facade.executeClientRequest(req);

        assertEquals(54, response.getNumberOfRejectedRows());
        assertEquals(0, response.getNumberOfInvalidRows());
        
        req = ClientRequest.builder()
                            .onDataset("frame1")
                            .withColumnType("boolean123", DomainType.NUMERIC)
                            .build();

        System.out.println(req.getNumberConstraintChecks().size());

        response = facade.executeClientRequest(req);

        assertEquals(0, response.getNumberOfRejectedRows());
        assertEquals(100, response.getNumberOfInvalidRows());
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
