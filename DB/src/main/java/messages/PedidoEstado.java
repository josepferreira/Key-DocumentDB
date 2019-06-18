package messages;

import nodes.KeysUniverse;

public class PedidoEstado {
    public String id;
    public KeysUniverse ku;

    public PedidoEstado(String id, KeysUniverse ku) {
        this.id = id;
        this.ku = ku;
    }

    public PedidoEstado(String id) {
        this.id = id;
    }
}
