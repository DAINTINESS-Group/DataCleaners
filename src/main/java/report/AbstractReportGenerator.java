package report;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import model.DatasetProfile;
import model.ServerRequest;
import model.ServerRequestResult;
import utils.ViolatingRowPolicy;

public abstract class AbstractReportGenerator implements IReportGenerator, Serializable{

    private static final long serialVersionUID = -8941830645847259570L;
	protected static FileWriter fileWriter;
    protected static int requestCounter;

    public void generateReport(DatasetProfile profile, String path) 
    {   
        requestCounter = 0;
        for (ServerRequest serverRequest : profile.getServerRequests())
        {
            generateReportFromServerRequest(profile.getDataset(), serverRequest, path);
            requestCounter++;
        }
    }

    private void generateReportFromServerRequest(Dataset<Row> dataSet, ServerRequest serverRequest, String path)
    {
        if (serverRequest.getRowChecks().size() == 0) return;
        ViolatingRowPolicy policy = serverRequest.getViolatingRowPolicy();
        switch (policy)
        {
            //WARN will just make the report. By default, we always make a report.
            case WARN:
                break;
            case ISOLATE:
                isolateRejectedEntries(dataSet, serverRequest,
                path + "\\" +  serverRequest.getProfile().getAlias() + "-request" + requestCounter + "-passedEntries.tsv",
                path + "\\" +  serverRequest.getProfile().getAlias()  + "-request" + requestCounter + "-rejectedEntries.tsv");
                break;
            case PURGE:
                purgeRejectedEntries(dataSet, serverRequest,
                path + "\\" +  serverRequest.getProfile().getAlias()  + "-request" + requestCounter + "-passedEntries.tsv");
                break;
        }
        generateWarningLog(serverRequest, path);

    }

    private void isolateRejectedEntries(Dataset<Row> dataSet, ServerRequest serverRequest,
                                          String passedEntriesPath, String rejectedEntriesPath)
    {
        Dataset<Row> passedRowIds = getPassedRowIds(serverRequest);
        Dataset<Row> passedRows = dataSet.join(passedRowIds, "_id", "inner").drop("_id");
        Dataset<Row> rejectedRows = dataSet.join(passedRowIds, "_id", "anti").drop("_id");

        writeDatasetToFile(passedRows, passedEntriesPath);
        writeDatasetToFile(rejectedRows, rejectedEntriesPath);

    }
    
    private void purgeRejectedEntries(Dataset<Row> dataSet, ServerRequest serverRequest,
                                          String passedEntriesPath)
    {
        Dataset<Row> passedRowIds = getPassedRowIds(serverRequest);
        Dataset<Row> passedRows = dataSet.join(passedRowIds, "_id", "inner").drop("_id");

        writeDatasetToFile(passedRows, passedEntriesPath);
    }

    private Dataset<Row> getPassedRowIds(ServerRequest serverRequest)
    {
        ServerRequestResult result = serverRequest.getRequestResult();
        ArrayList<String> queries = new ArrayList<String>();
        for (int i = 0; i < result.getRowCheckTypes().size(); i++)
        {
            queries.add("c" + i + " = 'PASSED'");
        }
        return result.getRowCheckResults().select("_id").where(String.join(" AND ", queries));
    }

    private void writeDatasetToFile(Dataset<Row> ds, String outputPath)
    {
        try
        {
            createOrRemakeFile(outputPath);
            fileWriter = new FileWriter(new File(outputPath), true);
            fileWriter.write(String.join("\t",ds.columns()) + "\n");
            ds.foreach(row -> { fileWriter.write(row.mkString("\t") + "\n");} );
            fileWriter.close();
        }
        catch (Exception e)
        {
            System.out.println("Write Dataset to File Exception!\n" + e);
        }
        
    }

    protected abstract void generateWarningLog(ServerRequest serverRequest, String outputFilePath);

    /**
     * Creates a new file. If the file exists, it is deleted and re-created.
     * @param filePath
     * @throws SecurityException -- If an I/O error occurred
     * @throws IOException - If a security manager exists and its <code>{@link
     *          java.lang.SecurityManager#checkWrite(java.lang.String)}</code>
     *          method denies write access to the file
     */
    protected void createOrRemakeFile(String filePath) throws SecurityException, IOException
    {
        File outputFile = new File(filePath);
        if (outputFile.exists())
        {
            outputFile.delete();
            outputFile.createNewFile();
        }
    }
}
