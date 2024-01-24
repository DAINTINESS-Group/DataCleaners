package engine;

import model.ClientRequest;
import model.ClientRequestResponse;
import model.DatasetProfile;
import model.ServerRequest;
import model.ServerRequestResult;
import report.IReportGenerator;
import report.ReportGeneratorFactory;
import utils.RegistrationResponse;
import utils.ReportType;

public class DataCleanerFacade implements IDataCleanerFacade {

    private DatasetRegistrationController datasetController= new DatasetRegistrationController();
    private ClientToServerRequestTranslator clientToServerTranslator = new ClientToServerRequestTranslator();
    private ServerToClientResponseTranslator serverToClientTranslator = new ServerToClientResponseTranslator();
    private ServerRequestExecutor serverRequestExecutor = new ServerRequestExecutor();


    public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader)
    {
        return datasetController.registerDataset(path, alias, hasHeader);
    }

    public RegistrationResponse registerDataset(String username, String password, String databaseType, 
                                        String url, String tableName, String alias)
    {
        return datasetController.registerDataset(username, password, databaseType, url, tableName, alias);
    }


    public ClientRequestResponse executeClientRequest(ClientRequest clientRequest)
    {
        DatasetProfile targetProfile = getProfile(clientRequest.getTargetDataset());
        ServerRequest serverRequest = translateClientToServerRequest(clientRequest);
        targetProfile.addServerRequest(serverRequest);
        serverRequest.setProfile(targetProfile);

        ServerRequestResult serverResult = executeServerRequest(serverRequest);

        return replyToClientRequest(serverResult);
    }


    private ServerRequest translateClientToServerRequest(ClientRequest clientReq)
    {
        return clientToServerTranslator.createServerRequest(clientReq, datasetController.getProfiles());
    }

    private ServerRequestResult executeServerRequest(ServerRequest serverReq)
    {
        return serverRequestExecutor.executeServerRequest(serverReq);
    }

    private ClientRequestResponse replyToClientRequest(ServerRequestResult serverResult)
    {
        return serverToClientTranslator.translateServerResponse(serverResult);
    }

    public void generateReport(String frameAlias, String outputDirectoryPath, ReportType type)
    {
        ReportGeneratorFactory reportGeneratorFactory = new ReportGeneratorFactory();
        IReportGenerator reportGenerator = reportGeneratorFactory.createReportGenerator(type);
        DatasetProfile profile = getProfile(frameAlias);

        reportGenerator.generateReport(profile, outputDirectoryPath);
    }

    //TO-DO: Add error catching when profile doesnt exist?
    private DatasetProfile getProfile(String alias)
    {
        return datasetController.getProfile(alias);
    }
}
