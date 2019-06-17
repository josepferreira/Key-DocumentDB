package messages;

import nodes.KeysUniverse;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.TreeSet;

public class RestartReply {

    public TreeMap<KeysUniverse, String> keys;

    public RestartReply(TreeMap<KeysUniverse, String> keys) {
        this.keys = keys;
    }
}
