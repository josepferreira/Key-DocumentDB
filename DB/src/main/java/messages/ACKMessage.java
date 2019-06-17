package messages;

public class ACKMessage {
    public String id;
    public boolean put;

    public ACKMessage(String id, boolean put) {
        this.id = id;
        this.put = put;
    }
}
