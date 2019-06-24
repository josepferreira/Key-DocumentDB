package messages;

public class PutReply {

    public boolean success;
    public String id;

    public PutReply(String id, boolean success) {
        this.id = id; this.success = success;
    }
}
