package report;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import model.ServerRequest;
import model.ServerRequestResult;

/**
 * This class is responsible for generating a report from a <code>DatasetProfile</code> with at least one
 * completed <code>ServerRequest</code>. Log reports are generated in TXT format, one for each <code>ServerRequest</code>
 */
public class StaticTxtReportGenerator extends AbstractReportGenerator {
    
    private static final long serialVersionUID = 4847761730341782802L;

	protected void generateWarningLog(ServerRequest serverRequest, String outputDirectory)
    {
        ServerRequestResult result = serverRequest.getRequestResult();
        Dataset<Row> results = result.getRowCheckResults();
        ArrayList<String> rowChecks = result.getRowCheckTypes();
        String outputFilePath = outputDirectory + "\\" + serverRequest.getProfile().getAlias() + "-request" + requestCounter + "-log.txt";
        try
        {
            createOrRemakeFile(outputFilePath);

            int headerModifier = serverRequest.getProfile().hasFileHeader() ? 1 : 0;
            fileWriter = new FileWriter(new File(outputFilePath), true);
            for (int i = 0; i < rowChecks.size(); i++)
            {
                Dataset<Row> rejectedRows = results.select("_id").where("c" + i + " = 'REJECTED'");
                Dataset<Row> invalidRows = results.select("_id").where("c" + i + " != 'REJECTED' AND c" + i + " != 'PASSED'");

                fileWriter.write(rowChecks.get(i) + "\nRejected Lines: ");
                rejectedRows.foreach(row -> 
                {
                    fileWriter.write(row.getInt(0) + headerModifier +  " ");
                });
                fileWriter.write("\nInvalid Lines: ");
                invalidRows.foreach(row -> 
                {
                    fileWriter.write(row.getInt(0) + headerModifier + " ");
                });

                fileWriter.write("\n\n");
            }
            fileWriter.close();
        }
        catch(Exception e)
        {
            System.out.println("Static TXT Warning Log Generation Error: \n" + e);
        }
    }

}
