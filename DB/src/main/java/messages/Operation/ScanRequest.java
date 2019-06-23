package messages.Operation;

import Configuration.KeysUniverse;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Predicate;

public class ScanRequest {
    public String id;
    public ArrayList<Predicate<JSONObject>> filtros;
    public HashMap<Boolean,ArrayList<String>> projecoes;
    public KeysUniverse ku;
    public int nrMaximo;
    public Object ultimaChave;
    public String endereco;

    public ScanRequest(String id, String endereco, ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projecoes, KeysUniverse ku, int nrMaximo, Object ultimaChave) {
        this.id = id;
        this.filtros = filtros;
        this.projecoes = projecoes;
        this.ku = ku;
        this.nrMaximo = nrMaximo;
        this.ultimaChave = ultimaChave;
        this.endereco = endereco;
    }
}
