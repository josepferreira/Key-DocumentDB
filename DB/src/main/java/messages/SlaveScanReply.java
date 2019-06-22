package messages;


import nodes.KeysUniverse;
import org.json.JSONObject;

import java.util.LinkedHashMap;

public class SlaveScanReply {
    public LinkedHashMap<Object, JSONObject> docs;
    public KeysUniverse universe;
    public String id;
    public Object ultimaChave;

    public SlaveScanReply(LinkedHashMap<Object, JSONObject> docs, KeysUniverse universe, String id, Object ultimaChave) {
        this.docs = docs;
        this.universe = universe;
        this.id = id;
        this.ultimaChave = ultimaChave;
    }
}
