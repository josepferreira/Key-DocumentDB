package messages;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Predicate;

public class RemoveRequest {
    public long key; //para já só uma key, depois pode ter várias
    public String id;
    public ArrayList<Predicate<JSONObject>> filtros = null;
    public ArrayList<String> projecoes = null;


    public RemoveRequest(String id, long key) {
        this.id = id; this.key = key;
    }

    public RemoveRequest(String id, long key, ArrayList<Predicate<JSONObject>> filtros, ArrayList<String> projecoes) {
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
