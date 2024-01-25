package model;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
