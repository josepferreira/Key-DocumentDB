package nodes;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public class Scan {
    String id;
    CompletableFuture<LinkedHashMap<Long, JSONObject>> cf;
    HashMap<SlaveIdentifier,CompletableFuture<Void>> cfsSlaves= new HashMap<>();
    TreeMap<KeysUniverse,SlaveIdentifier> slaves;
    HashMap<SlaveIdentifier, LinkedHashMap<Long, JSONObject>> respostas = new HashMap<>();
    public ArrayList<Predicate<JSONObject>> filtros;
    public HashMap<Boolean,ArrayList<String>> projeções;

    public Scan(String id, CompletableFuture<LinkedHashMap<Long, JSONObject>> cf,
                ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean,ArrayList<String>> projecoes) {
        this.id = id;
        this.cf = cf;
        this.filtros = filtros;
        this.projeções = projecoes;
    }

    public void adicionaSlaves(TreeMap<KeysUniverse,SlaveIdentifier> s){
        slaves = s;

        for(SlaveIdentifier si: slaves.values()){
            cfsSlaves.put(si,new CompletableFuture<>());
        }
    }
}
