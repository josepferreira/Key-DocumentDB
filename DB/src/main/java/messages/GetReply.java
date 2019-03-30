package messages;

import org.json.JSONObject;

public class GetReply {
    public long key;
    public JSONObject value;

    public GetReply(long key, JSONObject value) {
        this.key = key;
        this.value = value;
    }
}
