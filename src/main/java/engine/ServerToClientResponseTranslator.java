package engine;

import engine.clientRequest.ClientRequestResponse;
import model.ServerRequestResult;

/**
 * This class is responsible for translating a <code>ServerRequestResult</code> of an executed <code>ServerRequest</code>
 * into a <code>ClientRequestResponse</code>. The response is returned to the client as part of the execution process.
 * @see ClientRequestResponse
 * @see ServerRequestResult
 */
public class ServerToClientResponseTranslator {
    
    public ClientRequestResponse translateServerResponse(ServerRequestResult result)
    {
        return new ClientRequestResponse(result.getInvalidRows() == 0, result.getRejectedRows(), result.getInvalidRows());
    }
}
