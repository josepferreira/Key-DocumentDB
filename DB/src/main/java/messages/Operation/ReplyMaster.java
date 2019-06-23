package messages.Operation;

import nodes.SlaveIdentifier;

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
