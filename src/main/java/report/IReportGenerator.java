package report;

import model.DatasetProfile;

public interface IReportGenerator {
    public void generateReport(DatasetProfile profile, String path);
}
