package messages.Replication;

import messages.Operation.PutRequest;
import messages.Operation.RemoveRequest;
import org.json.JSONObject;

public class UpdateMessage {
    public Object key;
    public JSONObject value;
    public String id;
    public boolean resposta;
    public PutRequest pr;
    public RemoveRequest rr;

    public UpdateMessage(Object key, JSONObject value, String id, boolean resposta, PutRequest pr) {
        this.key = key;
        this.value = value;
        this.id = id;
        this.resposta = resposta;
        this.pr = pr;
        this.rr = null;
    }

    public UpdateMessage(Object key, JSONObject value, String id, boolean resposta, RemoveRequest rr) {
        this.key = key;
        this.value = value;
        this.id = id;
        this.resposta = resposta;
        this.rr = rr;
        this.pr = null;
    }
}