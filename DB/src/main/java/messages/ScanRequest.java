package messages;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Predicate;

public class ScanRequest {
    public String id;
    public ArrayList<Predicate<JSONObject>> filtros;
    public HashMap<Boolean,ArrayList<String>> projeções;

    public ScanRequest(String id, ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projeções) {
        this.id = id;
        this.filtros = filtros;
        this.projeções = projeções;
    }
}
