package messages;


import nodes.KeysUniverse;
import org.json.JSONObject;

import java.util.LinkedHashMap;

public class SlaveScanReply {
    public LinkedHashMap<Long, JSONObject> docs;
    public KeysUniverse universe;
    public String id;

    public SlaveScanReply(LinkedHashMap<Long, JSONObject> docs, KeysUniverse universe, String id) {
        this.docs = docs;
        this.universe = universe;
        this.id = id;
    }
}
