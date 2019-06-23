package messages.Flexibility;

import Configuration.KeysUniverse;

import java.util.HashSet;

public class JoinGroup {

    public HashSet<KeysUniverse> chaves;
    public String id;

    public JoinGroup(HashSet<KeysUniverse> chaves, String id) {
        this.chaves = chaves;
        this.id = id;
    }
}
