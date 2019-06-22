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
    public int ultimoID;
    public HashMap<String, String> dockers;
    public boolean balanceamentoCarga = false;
    public HashMap<String,InfoMonitorizacao> infoSlaves;
    public HashMap<String,HashSet<KeysUniverse>> esperaEntra;
    public HashMap<String,HashSet<KeysUniverse>> esperaSai;

    public EstadoMaster(String id, int nSlaves, TreeMap<KeysUniverse, SlaveIdentifier> slaves, HashSet<String> start, HashMap<String, TreeSet<KeysUniverse>> keysSlaves, int ultimoID, HashMap<String, String> dockers, boolean balanceamentoCarga, HashMap<String, InfoMonitorizacao> infoSlaves, HashMap<String, HashSet<KeysUniverse>> esperaEntra, HashMap<String, HashSet<KeysUniverse>> esperaSai) {
        this.id = id;
        this.nSlaves = nSlaves;
        this.slaves = slaves;
        this.start = start;
        this.keysSlaves = keysSlaves;
        this.ultimoID = ultimoID;
        this.dockers = dockers;
        this.balanceamentoCarga = balanceamentoCarga;
        this.infoSlaves = infoSlaves;
        this.esperaEntra = esperaEntra;
        this.esperaSai = esperaSai;
    }
}
