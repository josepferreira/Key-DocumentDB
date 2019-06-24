package messages;

import nodes.KeysUniverse;

public class PedidoEstado {
    public String id;
    public KeysUniverse ku;
    public boolean estadoAMeio = false;
    public long lastKey;

    public PedidoEstado(String id, KeysUniverse ku) {
        this.id = id;
        this.ku = ku;
    }

    public PedidoEstado(String id) {
        this.id = id;
    }

    public PedidoEstado(String id, KeysUniverse ku, long lastKey) {
        this.id = id;
        this.ku = ku;
        this.lastKey = lastKey;
        this.estadoAMeio = true;
    }
}
