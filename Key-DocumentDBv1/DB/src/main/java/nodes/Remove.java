package nodes;

import messages.RemoveRequest;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public class Remove {
    public RemoveRequest request;
    public CompletableFuture<Boolean> cf;
    public ArrayList<Predicate<JSONObject>> filtros;
    public ArrayList<String> projecoes;

    public Remove(RemoveRequest request, CompletableFuture<Boolean> cf) {
        this.request = request;
        this.cf = cf;
    }

    public Remove(RemoveRequest request, CompletableFuture<Boolean> cf, ArrayList<Predicate<JSONObject>> filtros, ArrayList<String> projecoes) {
        this.request = request;
        this.cf = cf;
        this.filtros = filtros;
        this.projecoes = projecoes;
    }
}
