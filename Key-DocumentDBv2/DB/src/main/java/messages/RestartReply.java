package messages;

import nodes.KeysUniverse;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.TreeSet;

public class RestartReply {

    public TreeMap<KeysUniverse, String> keys;
    public String id;

    public RestartReply(String id, TreeMap<KeysUniverse, String> keys) {
        this.id = id; this.keys = keys;
    }
}
