package messages;

import org.json.JSONObject;

public class PutRequest{

    public long key;
    public JSONObject value;

    public PutRequest(long key, JSONObject value) {
        this.key = key;
        this.value = value;
    }
}
