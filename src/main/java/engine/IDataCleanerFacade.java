package engine;


import engine.clientRequest.ClientRequest;
import engine.clientRequest.ClientRequestResponse;
import utils.RegistrationResponse;
import utils.ReportType;

public interface IDataCleanerFacade {
    public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader);
    public RegistrationResponse registerDataset(String databaseUsername, String databasePassword, String databaseType, 
                                                String url, String tableName, String alias);
    public ClientRequestResponse executeClientRequest(ClientRequest clientRequest);
    public void generateReport(String profileAlias, String outputDirectorPath, ReportType reportType);
}
