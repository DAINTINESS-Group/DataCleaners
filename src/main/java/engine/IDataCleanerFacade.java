package engine;


import model.ClientRequest;
import model.ClientRequestResponse;
import utils.RegistrationResponse;

public interface IDataCleanerFacade {
    public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader);
    public RegistrationResponse registerDataset(String databaseUsername, String databasePassword, String databaseType, 
                                                String url, String tableName, String alias);
    public ClientRequestResponse executeClientRequest(ClientRequest clientRequest);
}
