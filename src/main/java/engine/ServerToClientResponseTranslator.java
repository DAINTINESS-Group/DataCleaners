package engine;

import model.ClientRequestResponse;
import model.ServerRequestResult;
import model.rowcheckresults.IRowCheckResult;

public class ServerToClientResponseTranslator {
    
    public ClientRequestResponse translateServerResponse(ServerRequestResult result)
    {
        boolean hasAnyInvalidChecks = false;
        for (IRowCheckResult r : result.getRowCheckResults())
        {
            if (!r.isSuccesful()) { hasAnyInvalidChecks = true; break; }
        }

        return new ClientRequestResponse(!hasAnyInvalidChecks, result.getRejectedRows(),result.getInvalidRows());
    }
}
