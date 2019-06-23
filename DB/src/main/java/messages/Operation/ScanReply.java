package messages.Operation;

import Configuration.KeysUniverse;
import nodes.SlaveIdentifier;

import java.util.TreeMap;

public class ScanReply {
    public String id;
    public TreeMap<KeysUniverse, SlaveIdentifier> slaves = new TreeMap<>();

    public ScanReply(String id, TreeMap<KeysUniverse, SlaveIdentifier> slaves) {
        this.id = id;
        this.slaves = slaves;
    }
}
