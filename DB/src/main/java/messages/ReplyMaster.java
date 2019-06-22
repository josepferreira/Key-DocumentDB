package messages;

import nodes.KeysUniverse;
import nodes.SlaveIdentifier;

import java.util.HashMap;

public class ReplyMaster {
    public SlaveIdentifier slave;
    public String id;
    public Object key;

    public ReplyMaster(String id, SlaveIdentifier si, Object key) {
        this.id = id;
        this.slave = si;
        this.key = key;
    }
}
