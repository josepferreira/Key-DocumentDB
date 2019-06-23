package Operations;

import messages.Operation.GetRequest;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public class Get {
    public GetRequest request;
    public CompletableFuture<JSONObject> cf;
    public ArrayList<Predicate<JSONObject>> filtros;
    public HashMap<Boolean,ArrayList<String>> projecoes;

    public Get(GetRequest request, CompletableFuture<JSONObject> cf) {
        this.request = request;
        this.cf = cf;
    }

    public Get(GetRequest request, CompletableFuture<JSONObject> cf, ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projecoes) {
        this.request = request;
        this.cf = cf;
        this.filtros = filtros;
        this.projecoes = projecoes;
    }
}
