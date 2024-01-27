package report;

import model.DatasetProfile;
/**
 * Classes implementing this interface are responsible for generating reports. They are able to accept
 * a <code>DatasetProfile</code> and a path to a directory, where all <code>ServerRequestResult</code>
 * are used to generate reports in the corresponding format.
 */
public interface IReportGenerator {
    public void generateReport(DatasetProfile profile, String path);
}
