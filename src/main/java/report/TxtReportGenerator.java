package report;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import model.ServerRequest;
import model.ServerRequestResult;

@Deprecated
public class TxtReportGenerator extends AbstractReportGenerator {

    private static final long serialVersionUID = 7932155593757703695L;

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
            for (int i = 0; i < rowChecks.size(); i++)
            {
                Dataset<Row> rejectedRows = results.select("_id").where("c" + i + " = 'REJECTED'");
                Dataset<Row> invalidRows = results.select("_id").where("c" + i + " != 'REJECTED' AND c" + i + " != 'PASSED'");

                writeToFile(rowChecks.get(i) + "\nRejected Lines: ", outputFilePath);
                writeIdsToFile(rejectedRows, outputFilePath, headerModifier);
                writeToFile("\nInvalid Lines: ", outputFilePath);
                writeIdsToFile(invalidRows, outputFilePath, headerModifier);

                writeToFile("\n\n", outputFilePath);
            }
        }
        catch(Exception e)
        {
            System.out.println("TXT Warning Log Generation Error:\n" + e);
        }
    }

    private void writeIdsToFile(Dataset<Row> dataSet, String outputPath, int lineModifier)
    {
        dataSet.foreachPartition(partition -> 
                {
                    try (FileWriter fileWriter = new FileWriter(new File(outputPath), true))
                    {
                        partition.forEachRemaining(row -> 
                        {
                            try
                            {
                                fileWriter.write(row.getInt(0) + lineModifier + " ");
                            }
                            catch (Exception e)
                            {
                                System.out.println("TXT WriteIdsToFile Generation Error:\n" + e);
                            }
                        });
                    }
                });
    }

    private void writeToFile(String text, String filePath) throws Exception
    {
        fileWriter = new FileWriter(new File(filePath), true);
        fileWriter.write(text);
        fileWriter.close();
    }

}
