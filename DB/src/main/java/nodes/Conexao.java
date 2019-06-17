package nodes;

import spread.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;

public class Conexao {

    public SpreadConnection connection = new SpreadConnection();
    public SpreadGroup group = new SpreadGroup();
    public String id;
    public String grupo;
    public String primario;
    public HashSet<String> secundarios = new HashSet<>();

    public Conexao(String id, String grupo) throws UnknownHostException, SpreadException {
        this.grupo = grupo;
        this.id = this.grupo.substring(0,6) + id;

        connection.connect(InetAddress.getByName("localhost"), 0, this.id, false, false);
        group.join(connection,this.grupo);

    }

    public void desconectar() throws SpreadException {
        connection.disconnect();
    }

    public void atualiza(HashSet<String> membros){

        primario = Collections.min(membros);
        membros.remove(primario);
        secundarios.clear();
        secundarios.addAll(membros);

    }

    public void addAML(AdvancedMessageListener aml){
        connection.add(aml);
    }

    @Override
    public String toString() {
        return "Conexao{" +
                "id='" + id + '\'' +
                ", grupo='" + grupo + '\'' +
                '}';
    }
}
