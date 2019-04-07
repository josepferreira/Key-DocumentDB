package messages;


import org.json.JSONObject;

import java.util.LinkedHashMap;

public class SlaveScanReply {
    public LinkedHashMap<Long, JSONObject> docs;
    public String id;

    public SlaveScanReply(LinkedHashMap<Long, JSONObject> docs, String id) {
        this.docs = docs;
        this.id = id;
    }
}
