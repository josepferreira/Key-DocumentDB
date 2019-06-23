package Global;

import Configuration.KeysUniverse;

public class StartReply {

    public String id;
    public KeysUniverse keys;

    public StartReply(String id, KeysUniverse keys) {
        this.id = id;
        this.keys = keys;
    }
}
