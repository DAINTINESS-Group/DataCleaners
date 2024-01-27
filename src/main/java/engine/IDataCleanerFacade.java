package engine;


import engine.clientRequest.ClientRequest;
import engine.clientRequest.ClientRequestResponse;
import utils.RegistrationResponse;
import utils.ReportType;

/**
 * Classes inheriting the <code>IDataCleanerFacade</code> take the position of the core communicator between the
 * Client and the other application components. It implements the Facade GoF pattern, granting the ability to
 * register <code>DatasetProfiles</code>, execute <code>ClientRequests</code> and generate reports.
 * @see model.DatasetProfile
 * @see ClientRequest
 */
public interface IDataCleanerFacade {
    public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader);
    public RegistrationResponse registerDataset(String databaseUsername, String databasePassword, String databaseType, 
                                                String url, String tableName, String alias);
    public ClientRequestResponse executeClientRequest(ClientRequest clientRequest);
    public void generateReport(String profileAlias, String outputDirectorPath, ReportType reportType);
}
