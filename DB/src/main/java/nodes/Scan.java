package nodes;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

public class Scan {
    String id;
    CompletableFuture<TreeMap<Long, JSONObject>> cf;
    HashMap<SlaveIdentifier,CompletableFuture<Void>> cfsSlaves= new HashMap<>();
    TreeMap<KeysUniverse,SlaveIdentifier> slaves;
    HashMap<SlaveIdentifier,TreeMap<Long, JSONObject>> respostas = new HashMap<>();

    public Scan(String id, CompletableFuture<TreeMap<Long, JSONObject>> cf) {
        this.id = id;
        this.cf = cf;
    }

    public void adicionaSlaves(TreeMap<KeysUniverse,SlaveIdentifier> s){
        slaves = s;

        for(SlaveIdentifier si: slaves.values()){
            cfsSlaves.put(si,new CompletableFuture<>());
        }
    }
}
