package messages;

import nodes.KeysUniverse;
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
    public long ultimaChave;

    public ScanRequest(String id, ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projecoes, KeysUniverse ku, int nrMaximo, long ultimaChave) {
        this.id = id;
        this.filtros = filtros;
        this.projecoes = projecoes;
        this.ku = ku;
        this.nrMaximo = nrMaximo;
        this.ultimaChave = ultimaChave;
    }
}
