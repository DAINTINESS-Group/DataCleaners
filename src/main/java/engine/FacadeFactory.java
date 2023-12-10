package engine;

public class FacadeFactory {
    
    public IDataCleanerFacade createDataCleanerFacade()
    {
        return new DataCleanerFacade();
    }
}
