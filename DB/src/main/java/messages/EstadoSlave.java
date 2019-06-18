package messages;

import nodes.Put;
import nodes.Remove;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.HashSet;

public class EstadoSlave {

    public String id;
    public HashMap<String, Put> putRequests = null;
    public HashMap<String, Remove> removeRequests;
    public HashMap<String, HashSet<String>> acks;

    public HashMap<Long, JSONObject> valores;
    public boolean last;

    public EstadoSlave(String id, HashMap<String, Put> putRequests, HashMap<String, Remove> removeRequests, HashMap<String, HashSet<String>> acks, HashMap<Long, JSONObject> valores, boolean last) {
        this.id = id;
        this.putRequests = putRequests;
        this.removeRequests = removeRequests;
        this.acks = acks;
        this.valores = valores;
        this.last = last;
    }

    public EstadoSlave(HashMap<Long, JSONObject> valores, boolean last){
        this.valores = valores;
        this.last = last;
    }
}
