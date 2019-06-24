package messages;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Predicate;

public class GetRequest {
    public long key; //para já só uma key, depois pode ter várias
    public String id;
    public ArrayList<Predicate<JSONObject>> filtros = null;
    public HashMap<Boolean,ArrayList<String>> projecoes = null;

    //depois vai ter seleções e projeções


    public GetRequest(String id, long key) {
        this.id = id; this.key = key;
    }

    public GetRequest(String id, long key, ArrayList<Predicate<JSONObject>> filtros) {
        this.key = key;
        this.id = id;
        this.filtros = filtros;
    }

    public GetRequest(String id, long key, HashMap<Boolean, ArrayList<String>> projecoes) {
        this.key = key;
        this.id = id;
        this.projecoes = projecoes;
    }

    public GetRequest(String id, long key, ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projecoes) {
        this.key = key;
        this.id = id;
        this.filtros = filtros;
        this.projecoes = projecoes;
    }

    @Override
    public String toString() {
        return "GetRequest{" +
                "key=" + key +
                ", id='" + id + '\'' +
                '}';
    }
}
