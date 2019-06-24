package messages;

import nodes.KeysUniverse;

import java.util.HashMap;

public class ReplyMaster {
    public String endereco;
    public KeysUniverse keys;
    public String id;
    public long key;

    public ReplyMaster(String id, String endereco, KeysUniverse keys, long key) {
        this.id = id;
        this.endereco = endereco;
        this.keys = keys;
        this.key = key;
    }
}
