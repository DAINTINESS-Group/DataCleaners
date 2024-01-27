package model;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * This class represents a Dataset, whether be from a CSV file or a database, as well as information about it, as well as
 * any executed requests and their results.
 * 
 * @param alias A string, unique amongst the registered DatasetProfiles.
 * @param filePath The path of the represented Dataset, if it came from a file. "DATABASE" otherwise.
 * @param fileHasHeader A boolean that represents whether the file has a header. False if extracted from Database. Used in report
 * generation for accurate line counting.
 * @param dataset A Spark <code>Dataset<Row></code>. Essentially the collection of rows of data from a database table or CSV.
 * @param serverRequests An array of all <code>ServerRequests</code> that have been executed (or are in the process of being executed).
 * 
 * @see ServerRequest
 * @see Dataset
 */
public class DatasetProfile implements Serializable{
    
    private static final long serialVersionUID = 1L;
	private String alias;
    private String filePath;
    private boolean fileHasHeader;
    private Dataset<Row> dataset;
    private ArrayList<ServerRequest> serverRequests;

    public DatasetProfile(String alias, Dataset<Row> dataset, String path, boolean fileHasHeader)
    {
        this.alias = alias;
        this.filePath = path;
        this.dataset = dataset;
        this.fileHasHeader = fileHasHeader;
        serverRequests = new ArrayList<ServerRequest>();
    }

    public void addServerRequest(ServerRequest request)
    {
        serverRequests.add(request);
    }

    public String getAlias() {
        return alias;
    }

    public String getFilePath() {
        return filePath;
    }

    public Dataset<Row> getDataset() {
        return dataset;
    }

    public ArrayList<ServerRequest> getServerRequests()
    {
        return serverRequests;
    }

    public boolean hasFileHeader()
    {
        return fileHasHeader;
    }


    
}
