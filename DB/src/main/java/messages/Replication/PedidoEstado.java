package messages.Replication;

import Configuration.KeysUniverse;

public class PedidoEstado {
    public String id;
    public KeysUniverse ku;
    public boolean estadoAMeio = false;
    public Object lastKey;
    public String endereco;

    public PedidoEstado(String id, KeysUniverse ku) {
        this.id = id;
        this.ku = ku;
        lastKey = null;
    }

    public PedidoEstado(String id) {
        this.id = id;
    }

    public PedidoEstado(String id, KeysUniverse ku, Object lastKey) {
        this.id = id;
        this.ku = ku;
        this.lastKey = lastKey;
        this.estadoAMeio = true;
    }

}
