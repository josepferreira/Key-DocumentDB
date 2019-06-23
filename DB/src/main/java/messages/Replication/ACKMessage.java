package messages.Replication;

import java.io.File;

public class ACKMessage {
    public String id;
    public boolean put;
    public Object key;

    public ACKMessage(String id, boolean put, Object key) {
        this.id = id;
        this.put = put;
        this.key = key;
    }
}
