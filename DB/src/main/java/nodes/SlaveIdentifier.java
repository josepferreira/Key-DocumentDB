package nodes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

//classe importante para se aceder diretamente ao endereco e ao conjunto de chaves do servidor especifico
public class SlaveIdentifier {
    public String endereco;
    public KeysUniverse keys;
    public HashMap<String,Integer> secundarios;
    public int proximo;

    public SlaveIdentifier(String endereco, KeysUniverse keys, HashMap<String,Integer> secundarios) {
        this.endereco = endereco;
        this.keys = keys;
        this.secundarios = secundarios;

        proximo = secundarios.size() + 1;
    }

    public SlaveIdentifier(String endereco, KeysUniverse keys) {
        this.endereco = endereco;
        this.keys = keys;
    }

    @Override
    public String toString() {
        return "SlaveIdentifier{" +
                "endereco='" + endereco + '\'' +
                ", keys=" + keys +
                ", secundarios=" + secundarios +
                ", proximo=" + proximo +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SlaveIdentifier that = (SlaveIdentifier) o;
        return this.endereco.equals(that.endereco);
    }

    @Override
    public int hashCode() {
        return this.endereco.hashCode();
    }
}
