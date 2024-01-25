package engine;

import engine.clientRequest.ClientRequestResponse;
import model.ServerRequestResult;

public class ServerToClientResponseTranslator {
    
    public ClientRequestResponse translateServerResponse(ServerRequestResult result)
    {
        return new ClientRequestResponse(result.getInvalidRows() == 0, result.getRejectedRows(), result.getInvalidRows());
    }
}
