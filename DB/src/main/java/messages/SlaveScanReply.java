package messages;


import org.json.JSONObject;

import java.util.TreeMap;

public class SlaveScanReply {
    public TreeMap<Long, JSONObject> docs;
    public String id;

    public SlaveScanReply(TreeMap<Long, JSONObject> docs, String id) {
        this.docs = docs;
        this.id = id;
    }
}
