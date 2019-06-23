package Support;

import Configuration.KeysUniverse;

import java.util.HashSet;

public class ParPrimarioSecundario{
    public KeysUniverse primario;
    public HashSet<KeysUniverse> secundarios;

    public ParPrimarioSecundario(KeysUniverse primario, HashSet<KeysUniverse> secundarios) {
        this.primario = primario;
        this.secundarios = secundarios;
    }

    @Override
    public String toString() {
        return "ParPrimarioSecundario{" +
                "primario=" + primario +
                ", secundarios=" + secundarios +
                '}';
    }
}
