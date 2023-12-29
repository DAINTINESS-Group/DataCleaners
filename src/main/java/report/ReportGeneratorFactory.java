package report;

import utils.ReportType;

public class ReportGeneratorFactory {
    
    public IReportGenerator createReportGenerator(ReportType type)
    {
        switch (type)
        {
            case TEXT:
                return createTxtReportGenerator();
            case MARKDOWN:
                return null;
            case HTML:
                return null;
            default:
                return null;
        }
    }

    public IReportGenerator createTxtReportGenerator()
    {
        return new StaticTxtReportGenerator();
    }
}
