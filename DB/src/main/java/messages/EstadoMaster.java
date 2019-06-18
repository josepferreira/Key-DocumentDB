package messages;

import nodes.KeysUniverse;
import nodes.SlaveIdentifier;

import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class EstadoMaster {
    public String id;
    public int nSlaves;
    public TreeMap<KeysUniverse,SlaveIdentifier> slaves;
    public HashSet<String> start;
    public HashMap<String, TreeSet<KeysUniverse>> keysSlaves;

    public EstadoMaster(String id, int nSlaves, TreeMap<KeysUniverse, SlaveIdentifier> slaves, HashSet<String> start, HashMap<String, TreeSet<KeysUniverse>> keysSlaves) {
        this.id = id;
        this.nSlaves = nSlaves;
        this.slaves = slaves;
        this.start = start;
        this.keysSlaves = keysSlaves;
    }
}
