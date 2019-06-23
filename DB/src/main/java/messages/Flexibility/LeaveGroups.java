package messages.Flexibility;

import Support.ParPrimarioSecundario;

import java.util.ArrayList;

public class LeaveGroups {

    public ArrayList<ParPrimarioSecundario> pares;
    public String id;

    public LeaveGroups(ArrayList<ParPrimarioSecundario> pares, String id) {
        this.pares = pares;
        this.id = id;
    }
}
