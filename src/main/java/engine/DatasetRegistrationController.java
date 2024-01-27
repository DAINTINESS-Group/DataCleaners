package engine;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;

import config.SparkConfig;
import model.DatasetProfile;
import utils.RegistrationResponse;

/**
 * This class is responsible for registering <code>Datasets</code> as <code>DatasetProfiles</code> and storing them
 * for future use by other components. We make use of {@link #registerDataset(String path, String alias, boolean hasHeader)} to register a
 * CSV file and {@link #registerDataset(String username, String password, String databaseType, String url, String tableName, String alias)} to register a table from a database.
 * All aliases must be unique. A <code>RegistrationResponse</code> is returned in both cases to represent if the registration
 * was succesful.
 * 
 * @see RegistrationResponse
 * @see DatasetProfile
 */
public class DatasetRegistrationController {
    
    private ArrayList<DatasetProfile> datasetProfiles = new ArrayList<DatasetProfile>();
    private SparkSession spark;

    public DatasetRegistrationController()
    {
        spark = new SparkConfig().getSparkSession();
    }

    public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader)
    {
        try
        {
            if (existsProfileWithAlias(alias)) return RegistrationResponse.ALIAS_EXISTS;

            Dataset<Row> df = spark.read().option("header",hasHeader).csv(path)
                                    .withColumn("_id", functions.row_number().over(Window.orderBy(functions.lit("A"))));
            DatasetProfile profile = new DatasetProfile(alias, df, path, hasHeader);
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
            
            Dataset<Row> df = spark.read().jdbc(url, tableName, properties)
                                .withColumn("_id", functions.row_number().over(Window.orderBy(functions.lit("A"))));
            DatasetProfile profile = new DatasetProfile(alias, df, "DATABASE", false);
            datasetProfiles.add(profile);
            return RegistrationResponse.SUCCESS;
        }
        catch (Exception e)
        {
            return RegistrationResponse.FAILURE;
        }
        
    }

    private boolean existsProfileWithAlias(String alias)
    {
        for (DatasetProfile profile : datasetProfiles)
        {
            if (profile.getAlias().equals(alias)) return true;
        }
        return false;
    }

    public ArrayList<DatasetProfile> getProfiles()
    {
        return datasetProfiles;
    }
    
    public DatasetProfile getProfile(String alias)
    {
        for (DatasetProfile profile : datasetProfiles)
        {
            if (profile.getAlias().equals(alias)) return profile;
        }
        return null;
    }
}
