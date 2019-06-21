package messages;

import nodes.KeysUniverse;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.TreeSet;

public class RestartReply {

    public TreeMap<KeysUniverse, String> keys;
    public HashMap<KeysUniverse , Boolean> podeEntrar;
    public String id;

    public RestartReply(TreeMap<KeysUniverse, String> keys, HashMap<KeysUniverse, Boolean> podeEntrar, String id) {
        this.keys = keys;
        this.podeEntrar = podeEntrar;
        this.id = id;
    }
}
