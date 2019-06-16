package nodes;

import spread.SpreadConnection;
import spread.SpreadException;
import spread.SpreadGroup;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Conexao {

    public SpreadConnection connection = new SpreadConnection();
    public SpreadGroup group = new SpreadGroup();
    public String id;
    public String grupo;

    public Conexao(String id, String grupo) throws UnknownHostException, SpreadException {
        this.grupo = grupo;
        this.id = this.grupo + id;

        connection.connect(InetAddress.getByName("localhost"), 0, this.id, false, false);
        group.join(connection,this.grupo);

    }

    public void desconectar() throws SpreadException {
        connection.disconnect();
    }

    @Override
    public String toString() {
        return "Conexao{" +
                "id='" + id + '\'' +
                ", grupo='" + grupo + '\'' +
                '}';
    }
}
