package engine;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import config.SparkConfig;
import model.ClientRequest;
import model.ClientRequestResponse;
import model.DatasetProfile;
import model.ServerRequest;
import model.ServerRequestResult;
import utils.RegistrationResponse;

public class DataCleanerFacade implements IDataCleanerFacade {

    private SparkSession spark;
    private ArrayList<DatasetProfile> datasetProfiles = new ArrayList<DatasetProfile>();

    private ClientToServerRequestTranslator clientToServerTranslator = new ClientToServerRequestTranslator();
    private ServerToClientResponseTranslator serverToClientTranslator = new ServerToClientResponseTranslator();

    public DataCleanerFacade()
    {
        spark = new SparkConfig().getSparkSession();
    }

    public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader)
    {
        try
        {
            if (existsProfileWithAlias(alias)) return RegistrationResponse.ALIAS_EXISTS;

            Dataset<Row> df = spark.read().option("header",hasHeader).csv(path).withColumn("_id", functions.monotonically_increasing_id());
            DatasetProfile profile = new DatasetProfile(alias, df, path);
            datasetProfiles.add(profile);

            return RegistrationResponse.SUCCESS;
        }
        catch (Exception e)
        {
            return RegistrationResponse.FAILURE;
        }
        
    }

    public RegistrationResponse registerDataset(String username, String password, String databaseType, 
                                        String url, String tableName, String alias)
    {
        try
        {
            if (existsProfileWithAlias(alias)) return RegistrationResponse.ALIAS_EXISTS;

            Properties properties = new Properties();
            properties.setProperty("driver", databaseType);
            properties.setProperty("username", username);
            properties.setProperty("password", password);
            
            Dataset<Row> df = spark.read().jdbc(url, tableName, properties).withColumn("_id", functions.monotonically_increasing_id());
           
            DatasetProfile profile = new DatasetProfile(alias, df, "DATABASE");
            datasetProfiles.add(profile);
            return RegistrationResponse.SUCCESS;
        }
        catch (Exception e)
        {
            return RegistrationResponse.FAILURE;
        }
        
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
        return clientToServerTranslator.createServerRequest(clientReq, datasetProfiles);
    }

    private ServerRequestResult executeServerRequest(ServerRequest serverReq)
    {
        return serverReq.execute();
    }

    private ClientRequestResponse replyToClientRequest(ServerRequestResult serverResult)
    {
        return serverToClientTranslator.translateServerResponse(serverResult);
    }

    private boolean existsProfileWithAlias(String alias)
    {
        for (DatasetProfile profile : datasetProfiles)
        {
            if (profile.getAlias().equals(alias)) return true;
        }
        return false;
    }

    private DatasetProfile getProfile(String alias)
    {
        for (DatasetProfile profile : datasetProfiles)
        {
            if (profile.getAlias().equals(alias)) return profile;
        }
        return null;
    }
}
