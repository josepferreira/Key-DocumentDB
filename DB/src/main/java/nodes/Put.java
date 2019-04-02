package nodes;

import messages.PutRequest;
import org.json.JSONObject;

import java.util.concurrent.CompletableFuture;

public class Put {
    PutRequest request;
    public CompletableFuture<Boolean> cf;

    public Put(PutRequest request, CompletableFuture<Boolean> cf) {
        this.request = request;
        this.cf = cf;
    }

}
