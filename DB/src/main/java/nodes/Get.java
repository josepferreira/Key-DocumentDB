package nodes;

import messages.GetRequest;
import org.json.JSONObject;

import java.util.concurrent.CompletableFuture;

public class Get {
    GetRequest request;
    public CompletableFuture<JSONObject> cf;

    public Get(GetRequest request, CompletableFuture<JSONObject> cf) {
        this.request = request;
        this.cf = cf;
    }

}
