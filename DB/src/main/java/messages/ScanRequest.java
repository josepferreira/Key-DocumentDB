package messages;

import nodes.KeysUniverse;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Predicate;

public class ScanRequest {
    public String id;
    public ArrayList<Predicate<JSONObject>> filtros;
    public HashMap<Boolean,ArrayList<String>> projeções;
    public KeysUniverse ku;

    public ScanRequest(String id, ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projeções, KeysUniverse ku) {
        this.id = id;
        this.filtros = filtros;
        this.projeções = projeções;
        this.ku = ku;
    }
}
