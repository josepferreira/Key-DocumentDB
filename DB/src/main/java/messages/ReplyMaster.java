package messages;

import nodes.KeysUniverse;

import java.util.HashMap;

public class ReplyMaster {
    public String endereco;
    public KeysUniverse keys;

    public ReplyMaster(String endereco, KeysUniverse keys) {
        this.endereco = endereco;
        this.keys = keys;
    }
}
