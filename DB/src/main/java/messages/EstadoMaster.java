package messages;

import nodes.KeysUniverse;
import nodes.SlaveIdentifier;

import java.util.HashSet;
import java.util.TreeMap;

public class EstadoMaster {
    public String id;
    public TreeMap<KeysUniverse, SlaveIdentifier> slaves = new TreeMap<>();
    public HashSet<String> start = new HashSet<>();

    public EstadoMaster(String id, TreeMap<KeysUniverse, SlaveIdentifier> slaves, HashSet<String> start) {
        this.id = id;
        this.slaves = slaves;
        this.start = start;
    }
}
