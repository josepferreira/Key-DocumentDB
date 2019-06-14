package messages;

import nodes.KeysUniverse;

import java.util.HashMap;
import java.util.TreeSet;

public class RestartReply {

    TreeSet<KeysUniverse> keys;
    HashMap<String,Integer> grupos;

    public RestartReply(TreeSet<KeysUniverse> keys, HashMap<String, Integer> grupos) {
        this.keys = keys;
        this.grupos = grupos;
    }
}
