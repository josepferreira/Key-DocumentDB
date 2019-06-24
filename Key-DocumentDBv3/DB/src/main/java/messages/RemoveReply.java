package messages;

import org.json.JSONObject;

public class RemoveReply {
    public boolean sucess;
    public String id;

    public RemoveReply(String id, boolean sucess) {
        this.sucess = sucess;
        this.id = id;
    }

    @Override
    public String toString() {
        return "RemoveReply{" +
                "sucess=" + sucess +
                ", id='" + id + '\'' +
                '}';
    }
}
