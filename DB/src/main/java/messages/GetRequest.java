package messages;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Predicate;

public class GetRequest {
    public Object key; //para já só uma key, depois pode ter várias
    public String id;
    public ArrayList<Predicate<JSONObject>> filtros = null;
    public HashMap<Boolean,ArrayList<String>> projecoes = null;
    public String endereco;

    //depois vai ter seleções e projeções


    public GetRequest(String id, Object key, String endereco) {
        this.id = id; this.key = key;
        this.endereco = endereco;
    }

    public GetRequest(String id, Object key, String endereco, ArrayList<Predicate<JSONObject>> filtros) {
        this.key = key;
        this.id = id;
        this.endereco = endereco;
        this.filtros = filtros;
    }

    public GetRequest(String id, Object key, String endereco, HashMap<Boolean, ArrayList<String>> projecoes) {
        this.key = key;
        this.id = id;
        this.endereco = endereco;
        this.projecoes = projecoes;
    }

    public GetRequest(String id, Object key, String endereco, ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projecoes) {
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
