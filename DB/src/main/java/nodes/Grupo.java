package nodes;

import com.google.common.primitives.Longs;
import com.sun.xml.internal.ws.client.SenderException;
import io.atomix.utils.serializer.Serializer;
import messages.*;
import org.json.JSONObject;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import spread.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

class ElementoFila{
    Object o;
    String origem;

    public ElementoFila(Object o, String origem) {
        this.o = o;
        this.origem = origem;
    }
}


public class Grupo {

    public SpreadConnection connection = new SpreadConnection();
    public SpreadGroup group = new SpreadGroup();
    public String id;
    public String grupo;
    public String primario;
    public HashSet<String> secundarios = new HashSet<>();
    public RocksDB rocksDB;
    private Options options;
    public KeysUniverse ku;
    public HashMap<String,Put> putRequests = new HashMap<>();
    public HashMap<String,Remove> removeRequests = new HashMap<>();
    public HashMap<String, HashSet<String>> acks = new HashMap<>();

    public boolean estadoRecuperado = false;
    public ArrayList<ElementoFila> fila = new ArrayList<>();


    public Grupo(String id, String grupo, KeysUniverse ku, String rocksDBFolder, AdvancedMessageListener aml) throws UnknownHostException, SpreadException {
        this.ku = ku;
        System.out.println("1");
        this.grupo = grupo;
        System.out.println("2: " + this.grupo);
        if(this.grupo.length() > 8)
            this.id = this.grupo.substring(0,8) + id;
        else
            this.id = this.grupo + id;
        System.out.println("3: " + this.id);

        connection.connect(InetAddress.getByName("localhost"), 0, this.id, false, true);
//        connection.add(aml);
        System.out.println("4");


        this.options = new Options().setCreateIfMissing(true);
        try {
            System.out.println("Criar rocks");
            rocksDB = RocksDB.open(options, rocksDBFolder + ku.getGrupo()/*"./localdb/" + id.replaceAll(":", "") + "-" + ku.toString() + "/"*/);
            System.out.println("Rocks criado");
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

    }

    public void desconectar() throws SpreadException {
        connection.disconnect();
    }

    public void atualiza(HashSet<String> membros){

        primario = Collections.min(membros);
        membros.remove(primario);
        secundarios.clear();
        secundarios.addAll(membros);

        System.out.println("Atualizou");
        System.out.println(primario);
        System.out.println(secundarios);

    }

    public void connecta(AdvancedMessageListener aml) throws SpreadException {
        connection.add(aml);
        System.out.println("GR: " + this.grupo);
        group.join(connection,this.grupo);
        SpreadGroup g = new SpreadGroup();
        g.join(connection,this.id);

    }

    @Override
    public String toString() {
        return "Grupo{" +
                ", id='" + id + '\'' +
                ", grupo='" + grupo + '\'' +
                ", primario='" + primario + '\'' +
                ", secundarios=" + secundarios +
                ", ku=" + ku +
                ", putRequests=" + putRequests +
                ", removeRequests=" + removeRequests +
                ", acks=" + acks +
                ", estadoRecuperado=" + estadoRecuperado +
                '}';
    }

    private JSONObject aplicaProjecao(JSONObject o, HashMap<Boolean, ArrayList<String>> p){
        ArrayList<String> f = p.get(false);
        if(f != null){
            for(String aux: f){
                o.remove(aux);
            }
        }
        ArrayList<String> t = p.get(true);
        if(f != null){
            JSONObject aux = o;
            o = new JSONObject();
            for(String a: f){
                o.put(a,aux.get(a));
            }
        }

        return o;
    }

    //função que cria o filtro a partir dos vários filtros
    private Predicate<JSONObject> filtro(ArrayList<Predicate<JSONObject>> filters) {

        Predicate<JSONObject> pred = filters.stream().reduce(Predicate::and).orElse(x -> true);
        pred = pred.negate();

        return pred;
    }

    //handler para responder ao pedido put, efetuado pelo stub
    public boolean put(PutRequest pr){

        ////se ainda n inseriu insere
        byte[] key = Longs.toByteArray(pr.key);
        try {
            rocksDB.put(key, pr.value.toString().getBytes());
            return true;
        } catch (RocksDBException e) {
            e.printStackTrace();
            System.out.println("Erro ao realizar put na BD local!");
        }

        return false;

    }


    // **** Handler para responder a um getefetuado pelo stub
    public JSONObject get(GetRequest gr){

        byte[] keys = Longs.toByteArray(gr.key);
        byte[] value = null;

        try {
            value = rocksDB.get(keys);

            if(value == null){
                return null;
            }else{
                if(gr.filtros == null && gr.projecoes == null){
                    //Vai ser um get normal
                    String ret = new String(value);
                    JSONObject json = new JSONObject(ret);
                    return json;

                }else{
                    if(gr.projecoes != null && gr.filtros != null){
                        //Vai ser um get com projeções e filtros
                        String ret = new String(value);
                        JSONObject json = new JSONObject(ret);

                        Predicate<JSONObject> filtros = this.filtro(gr.filtros);
                        if(filtros.test(json)){
                            //Passou no filtro
                            JSONObject jsonToReturn = this.aplicaProjecao(json, gr.projecoes);

                            return jsonToReturn;
                        }else{
                            //não passou no filtro, vai um null
                            return null;
                        }
                    }else{
                        if(gr.projecoes != null){
                            String ret = new String(value);
                            JSONObject json = new JSONObject(ret);

                            JSONObject jsonToReturn = this.aplicaProjecao(json, gr.projecoes);

                            return jsonToReturn;
                        }else{
                            //são gets apenas com filtros
                            String ret = new String(value);
                            JSONObject json = new JSONObject(ret);

                            Predicate<JSONObject> filtros = this.filtro(gr.filtros);
                            if(filtros.test(json)){
                                //Passou no filtro
                                return json;
                            }else{
                                //não passou no filtro, vai um null
                                return null;
                            }
                        }
                    }
                }
            }

        } catch (RocksDBException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("ERRO NA STRING: " + e.getMessage());
        }

        return null;

    }


    public boolean remove(RemoveRequest rr){

        byte[] keys = Longs.toByteArray(rr.key);

        try {
            byte[] value = rocksDB.get(keys);

            if(value == null){
                return false;
            }else {

                if (rr.filtros == null && rr.projecoes == null) {
                    //Vai ser um remove normal
                    rocksDB.delete(keys);

                    return true;

                } else {
                    if (rr.projecoes != null && rr.filtros != null) {
                        //Vai ser um get com projeções e filtros
                        String ret = new String(value);
                        JSONObject json = new JSONObject(ret);

                        Predicate<JSONObject> filtros = this.filtro(rr.filtros);
                        if (filtros.test(json)) {
                            //Passou no filtro
                            for(String s: rr.projecoes)
                                json.remove(s);

                            rocksDB.put(value, json.toString().getBytes());

                            return true;
                        } else {
                            //não passou no filtro, não pode eliminar
                            return false;
                        }
                    } else {
                        if (rr.projecoes != null) {
                            String ret = new String(value);
                            JSONObject json = new JSONObject(ret);

                            for(String s: rr.projecoes)
                                json.remove(s);

                            rocksDB.put(value, json.toString().getBytes());

                            return true;
                        } else {
                            //são gets apenas com filtros
                            String ret = new String(value);
                            JSONObject json = new JSONObject(ret);

                            Predicate<JSONObject> filtros = this.filtro(rr.filtros);
                            if (filtros.test(json)) {
                                //Passou no filtro
                                rocksDB.delete(keys);

                                return true;
                            } else {
                                //não passou no filtro, logo não pode eliminar
                                return false;
                            }
                        }
                    }

                    /*db.delete(keys);

                    RemoveReply rrp = new RemoveReply(rr.id, true);
                    ms.sendAsync(a, "removeReply", s.encode(rrp));*/

                }
            }

        } catch (RocksDBException e) {
            e.printStackTrace();
            System.out.println("DEU PROBLEMAS A ELIMINAR O VALOR ...");
        } catch (Exception e) {
            System.out.println("ERRO NA STRING: " + e.getMessage());
        }

        return false;

    }

    //scan para todos os objectos, sem projecções
    private ResultadoScan getScan(int nrMaximo, long ultimaChave, KeysUniverse ku) {
        System.out.println("----------------------------Novo pedido scan: " + ku + " ---------------------------");
        LinkedHashMap<Long,JSONObject> docs = new LinkedHashMap<>();
        RocksIterator iterador = rocksDB.newIterator();
        int quantos = 0;
        long chave = -1;
        long anterior = -1;
        if(ultimaChave == -1) {
            ultimaChave = ku.min;
            iterador.seek(Longs.toByteArray(ultimaChave));
            chave = ultimaChave;
        }
        else{
            iterador.seek(Longs.toByteArray(ultimaChave));
            if(!iterador.isValid()){
                return null;
            }
            iterador.next();
        }



        while (iterador.isValid()) {
            long k = Longs.fromByteArray(iterador.key());
            System.out.println("Key: " + k);
            if(k <= anterior){
                //n está neste universe
                System.out.println("Deu a volta");
                break;
            }
            anterior = k;
            chave = k;
            String v = new String(iterador.value());
            JSONObject json = new JSONObject(v);
            docs.put(k,json);
            quantos++;
            if(quantos >= nrMaximo){
                System.out.println("Atingi o máximo");
                break;
            }
            iterador.next();
        }
        System.out.println("---------------FIM-----------------------");
        return new ResultadoScan(chave,docs);
    }

    public ResultadoScan scan(ScanRequest sr){
        ResultadoScan docs = null; //n será muito eficiente, provavelmente por causa de andar sempre a mudar o map

        //de alguma forma faz o scan à bd, ver a melhor forma
        if(sr.filtros == null){
            if(sr.projecoes == null){
                docs = getScan(sr.nrMaximo, sr.ultimaChave,sr.ku);
            }
            /*else{
                docs = getScan(sr.projecoes);
            }
        }
        else{
            if(sr.projecoes == null){
                docs = getScan(filtro(sr.filtros));
            }
            else{
                docs = getScan(filtro(sr.filtros),sr.projecoes);
            }*/

        }

        return docs;

    }

    public boolean updateState(UpdateMessage um){

        ////se ainda n inseriu insere
        byte[] key = Longs.toByteArray(um.key);
        try {
            if(um.value != null) {
                rocksDB.put(key, um.value.toString().getBytes());
                return true;
            }else{
                rocksDB.delete(key);
                return true;
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
            System.out.println("Erro ao realizar put na BD local! Estranho, foi num update vindo do primario!!!");
        }
        return false;

    }


    private boolean put(Long k, JSONObject value){

        ////se ainda n inseriu insere
        byte[] key = Longs.toByteArray(k);
        try {
            rocksDB.put(key, value.toString().getBytes());
            return true;
        } catch (RocksDBException e) {
            e.printStackTrace();
            System.out.println("Erro ao realizar put na BD local!");
        }

        return false;

    }

    public void recuperaEstado(EstadoSlave es) {
        System.out.println("VOU COMEÇAR A RECUPERAR O ESTADO: " + (es.putRequests == null));
        System.out.println("\t\t\t\tVAMOS VER OS VALORES AGORA SEUS BOIS: " + es.valores);
        if (es.putRequests != null) {
            putRequests = es.putRequests;
            acks = es.acks;
            //falta remove requests
        }

        for(Map.Entry<Long,JSONObject> entry: es.valores.entrySet()){
            put(entry.getKey(),entry.getValue());
        }

        estadoRecuperado = es.last;
    }

    public void pedidoEstado(PedidoEstado pe, String sender, Serializer s) {

        long quantos = 0;
        long max = 20;
        HashMap<Long,JSONObject> map = new HashMap<>();
        EstadoSlave es = new EstadoSlave(pe.id,putRequests,removeRequests,acks,map,false,ku.min);

        RocksIterator iterador = rocksDB.newIterator();
        boolean enviei = false;

        while(iterador.isValid()){
            enviei = false;
            long k = Longs.fromByteArray(iterador.key());
            String v = new String(iterador.value());
            JSONObject json = new JSONObject(v);
            map.put(k,json);
            iterador.next();
            quantos++;
            if(quantos >= max){
                enviei = true;
                es.last = !iterador.isValid();
                es.valores = map;
                quantos = 0;

                SpreadMessage sm = new SpreadMessage();
                sm.setData(s.encode(es));
                sm.setReliable();
                sm.addGroup(sender);

                try {
                    connection.multicast(sm);
                } catch (SpreadException e) {
                    e.printStackTrace();
                }

                map.clear();
            }
        }

        if(!enviei) {
            es.last = !iterador.isValid();
            SpreadMessage sm = new SpreadMessage();
            es.valores = map;

            System.out.println("Enviar valores: " + es.valores);
            sm.setData(s.encode(es));
            sm.setReliable();
            sm.addGroup(sender);

            try {
                connection.multicast(sm);
            } catch (SpreadException e) {
                e.printStackTrace();
            }
        }
    }

    public void pedidoEstado(PedidoEstado pe, SpreadGroup sender, Serializer s) {

        long quantos = 0;
        long max = 20;
        HashMap<Long,JSONObject> map = new HashMap<>();
        EstadoSlave es = new EstadoSlave(pe.id,putRequests,removeRequests,acks,map,false,ku.min);

        RocksIterator iterador = rocksDB.newIterator();
        boolean enviei = false;
        long ultimaChave = ku.min;
        iterador.seek(Longs.toByteArray(ultimaChave));

        while(iterador.isValid()){
            enviei = false;
            long k = Longs.fromByteArray(iterador.key());
            String v = new String(iterador.value());
            JSONObject json = new JSONObject(v);
            map.put(k,json);
            System.out.println("VAMOS VER OS VALORES INTERMEDIOS: " + map);
            iterador.next();
            quantos++;
            if(quantos >= max){
                enviei = true;
                es.last = !iterador.isValid();
                es.valores = map;
                quantos = 0;

                SpreadMessage sm = new SpreadMessage();
                sm.setData(s.encode(es));
                sm.setReliable();
                sm.addGroup(sender);

                try {
                    connection.multicast(sm);
                } catch (SpreadException e) {
                    e.printStackTrace();
                }

                map.clear();
            }
        }

        if(!enviei) {
            es.last = !iterador.isValid();
            SpreadMessage sm = new SpreadMessage();
            es.valores = map;
            System.out.println(ku);
            System.out.println("Enviar valores: " + es.valores);
            sm.setData(s.encode(es));
            sm.setReliable();
            sm.addGroup(sender);

            try {
                connection.multicast(sm);
            } catch (SpreadException e) {
                e.printStackTrace();
            }
        }
    }
}
