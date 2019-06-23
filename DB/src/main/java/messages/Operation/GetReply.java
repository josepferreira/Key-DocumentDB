package messages.Operation;

import org.json.JSONObject;

public class GetReply {
    public Object key;
    public JSONObject value;
    public String id;

    public GetReply(String id, Object key, JSONObject value) {
        this.id = id;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "GetReply{" +
                "key=" + key +
                ", value=" + value +
                ", id='" + id + '\'' +
                '}';
    }
}