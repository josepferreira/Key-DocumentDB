package messages;


import nodes.KeysUniverse;
import org.json.JSONObject;

import java.util.LinkedHashMap;

public class SlaveScanReply {
    public LinkedHashMap<Long, JSONObject> docs;
    public KeysUniverse universe;
    public String id;
    public long ultimaChave;

    public SlaveScanReply(LinkedHashMap<Long, JSONObject> docs, KeysUniverse universe, String id, long ultimaChave) {
        this.docs = docs;
        this.universe = universe;
        this.id = id;
        this.ultimaChave = ultimaChave;
    }
}
