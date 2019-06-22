package messages;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Predicate;

public class RemoveRequest {
    public Object key; //para já só uma key, depois pode ter várias
    public String id;
    public ArrayList<Predicate<JSONObject>> filtros = null;
    public ArrayList<String> projecoes = null;
    public String endereco;


    public RemoveRequest(String id, Object key, String endereco) {
        this.id = id; this.key = key;
        this.endereco = endereco;
    }

    public RemoveRequest(String id, Object key, String endereco, ArrayList<Predicate<JSONObject>> filtros, ArrayList<String> projecoes) {
        this.key = key;
        this.id = id;
        this.endereco = endereco;
        this.filtros = filtros;
        this.projecoes = projecoes;
    }

    @Override
    public String toString() {
        return "GetRequest{" +
                "key=" + key +
                ", id='" + id + '\'' +
                ", endereco='" + endereco + '\'' +
                '}';
    }
}
