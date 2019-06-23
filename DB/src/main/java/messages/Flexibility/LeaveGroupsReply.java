package messages.Flexibility;

import Configuration.KeysUniverse;

import java.util.HashSet;

public class LeaveGroupsReply {

    public HashSet<KeysUniverse> primarios;
    public String id;

    public LeaveGroupsReply(HashSet<KeysUniverse> primarios, String id) {
        this.primarios = primarios;
        this.id = id;
    }
}
