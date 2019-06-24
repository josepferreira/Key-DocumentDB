package messages;

import java.io.File;

public class ACKMessage {
    public String id;
    public boolean put;
    public long key;

    public ACKMessage(String id, boolean put, long key) {
        this.id = id;
        this.put = put;
        this.key = key;
    }
}
