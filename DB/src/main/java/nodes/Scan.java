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
    CompletableFuture<Void> cf;
    HashMap<SlaveIdentifier,CompletableFuture<Void>> cfsSlaves= new HashMap<>();
    TreeMap<KeysUniverse,SlaveIdentifier> slaves;

    HashMap<KeysUniverse, LinkedHashMap<Long, JSONObject>> respostas = new HashMap<>();
    public ArrayList<Predicate<JSONObject>> filtros;
    public HashMap<Boolean,ArrayList<String>> projeções;
    public CompletableFuture<ArrayList<CompletableFuture<LinkedHashMap<Long,JSONObject>>>> cfs;
    public ArrayList<CompletableFuture<LinkedHashMap<Long,JSONObject>>> mycfs = new ArrayList<>();
    public TreeMap<KeysUniverse,Integer> indicesKeys = new TreeMap<>();

    public Scan(String id, CompletableFuture<Void> cf,
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
            mycfs.add(new CompletableFuture<LinkedHashMap<Long, JSONObject>>());
            indicesKeys.put(si.keys,mycfs.size()-1);
        }

        //já sei quais são os slaves, posso completar para retornar
        cfs.complete(mycfs);
        cf.complete(null);
    }
}
