package messages;

import nodes.KeysUniverse;

public class StartMessage {
    public KeysUniverse ku;
    public int id;

    public StartMessage(KeysUniverse ku, int id) {
        this.ku = ku;
        this.id = id;
    }
}
