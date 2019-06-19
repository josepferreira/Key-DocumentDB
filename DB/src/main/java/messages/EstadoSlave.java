package messages;

import nodes.Put;
import nodes.Remove;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

public class EstadoSlave {

    public String id;
    public LinkedHashMap<String, Object> requests = null;
    //public LinkedHashMap<String, Remove> removeRequests;
    public HashMap<String, HashSet<String>> acks;

    public HashMap<Long, JSONObject> valores;
    public boolean last;
    public long key;

    public EstadoSlave(String id, LinkedHashMap<String, Object> requests, HashMap<String, HashSet<String>> acks, HashMap<Long, JSONObject> valores, boolean last, long key) {
        this.id = id;
        this.requests = requests;
        //this.removeRequests = removeRequests;
        this.acks = acks;
        this.valores = valores;
        this.last = last;
        this.key = key;
    }

    public EstadoSlave(HashMap<Long, JSONObject> valores, boolean last, long key){
        this.valores = valores;
        this.last = last;
        this.key = key;
    }
}
