package nodes;

import messages.GetRequest;
import messages.ScanRequest;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public class Get {
    public GetRequest request;
    public CompletableFuture<JSONObject> cf;
    public ArrayList<Predicate<JSONObject>> filtros;
    public HashMap<Boolean,ArrayList<String>> projeções;

    public Get(GetRequest request, CompletableFuture<JSONObject> cf) {
        this.request = request;
        this.cf = cf;
    }

    public Get(GetRequest request, CompletableFuture<JSONObject> cf, ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projeções) {
        this.request = request;
        this.cf = cf;
        this.filtros = filtros;
        this.projeções = projeções;
    }
}
