package messages;

import org.json.JSONObject;

public class UpdateMessage {
    public Long key;
    public JSONObject value;
    public String id;
    public boolean resposta;
    public PutRequest pr;
    public RemoveRequest rr;

    public UpdateMessage(Long key, JSONObject value, String id, boolean resposta, PutRequest pr) {
        this.key = key;
        this.value = value;
        this.id = id;
        this.resposta = resposta;
        this.pr = pr;
    }

    public UpdateMessage(Long key, JSONObject value, String id, boolean resposta, RemoveRequest rr) {
        this.key = key;
        this.value = value;
        this.id = id;
        this.resposta = resposta;
        this.rr = rr;
    }
}