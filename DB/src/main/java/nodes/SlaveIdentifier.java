package nodes;

import java.util.*;

class Secundario implements Comparable{
    public String id;
    public String endereco;
    public boolean ativo;

    public Secundario(String id, String endereco, boolean ativo) {
        this.id = id;
        this.endereco = endereco;
        this.ativo = ativo;
    }


    @Override
    public int compareTo(Object o) {
        Secundario sc = (Secundario)o;
        return id.compareTo(sc.id);
    }
}
//classe importante para se aceder diretamente ao endereco e ao conjunto de chaves do servidor especifico
public class SlaveIdentifier {
    public String endereco;
    public String idPrimario;
    public boolean ativo = false;
    public KeysUniverse keys;
    public HashMap<String,Secundario> secundarios = new HashMap<>();
    public int proximo;

    public SlaveIdentifier(String id, KeysUniverse keys, HashMap<String,Integer> secundarios) {
        this.idPrimario = id;
        this.keys = keys;

        for(Map.Entry<String,Integer> k: secundarios.entrySet()){
            Secundario sc = new Secundario(k.getValue()+"",null,false);
            this.secundarios.put(k.getKey(),sc);
        }

        proximo = secundarios.size() + 1;
    }

    public SlaveIdentifier(String endereco, KeysUniverse keys) {
        this.endereco = endereco;
        this.keys = keys;
    }

    public void entra(String id, String end){
        if(id.equals(idPrimario)){
            ativo = true;
            endereco = end;
        }
        else{
            Secundario sc = secundarios.get(id);
            if(sc != null){
                sc.ativo = true;
                sc.endereco = end;
            }
            else {
                System.out.println("O slave n aparece aqui!!!");
            }
        }
    }

    public void sai(String id){
        if(id.equals(idPrimario)){
            ativo = false;
        }
        else{
            Secundario sc = secundarios.get(id);
            if(sc != null){
                sc.ativo = false;
            }
            else {
                System.out.println("O slave n aparece aqui!!!");
            }
        }
    }

    public String primario(){
        if(ativo){
            return endereco;
        }

        Optional<Secundario> p = secundarios.values().stream().filter(a -> a.ativo).min(Secundario::compareTo);

        if(p.isPresent()){
            return p.get().endereco;
        }

        return null;

    }

    @Override
    public String toString() {
        return "SlaveIdentifier{" +
                "endereco='" + endereco + '\'' +
                "idPrimario='" + idPrimario + '\'' +
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
