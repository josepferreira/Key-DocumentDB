package messages;

import org.json.JSONObject;

public class PutRequest{

    public long key;
    public JSONObject value;
    public String id;

    public PutRequest(String id, long key, JSONObject value) {
        this.id = id;
        this.key = key;
        this.value = value;
    }
}
