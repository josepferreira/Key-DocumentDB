package nodes;

import com.google.common.primitives.Longs;
//import com.sun.xml.internal.ws.client.SenderException;
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

class RespostaRemove{
    public boolean resposta;
    public JSONObject jsonValue;
    public Object key;

    public RespostaRemove(boolean resposta, JSONObject jsonValue, Object key) {
        this.resposta = resposta;
        this.jsonValue = jsonValue;
        this.key = key;
    }
}

class ElementoFila{
    public Object o;
    public String origem;

    public ElementoFila(Object o, String origem) {
        this.o = o;
        this.origem = origem;
    }
}


public class Grupo {

    public SpreadConnection connection = new SpreadConnection();
    public SpreadGroup group = new SpreadGroup();
    public SpreadGroup myGroup = new SpreadGroup();
    public String id;
    public String grupo;
    public String primario;
    public HashSet<String> secundarios = new HashSet<>();
    public RocksDB rocksDB;
    private Options options;
    public KeysUniverse ku;
    public LinkedHashMap<String, Object> requests = new LinkedHashMap<>();
    //public LinkedHashMap<String,Put> putRequests = new LinkedHashMap<>();
    //public LinkedHashMap<String,Remove> removeRequests = new LinkedHashMap<>();
    public HashMap<String, HashSet<String>> acks = new HashMap<>();

    public boolean estadoRecuperado = false;
    public ArrayList<ElementoFila> fila = new ArrayList<>();
    public String origemEstado;
    public Object lastKey;
    public boolean estadoAMeio = false;

    //Para tratar da monitorização
    public int escritas = 0;
    public int leituras = 0;


    public AdvancedMessageListener aml;


    public Grupo(String id, String grupo, KeysUniverse ku, String rocksDBFolder, AdvancedMessageListener aml) throws UnknownHostException, SpreadException {
        this.ku = ku;
        this.grupo = grupo;
        if(this.grupo.length() > 8)
            this.id = this.grupo.substring(0,8) + id;
        else
            this.id = this.grupo + id;

//        connection.add(aml);


        this.options = new Options().setCreateIfMissing(true);
        try {
            rocksDB = RocksDB.open(options, rocksDBFolder + ku.getGrupo()/*"./localdb/" + id.replaceAll(":", "") + "-" + ku.toString() + "/"*/);
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


    }

    public void connecta(AdvancedMessageListener aml) throws SpreadException {
        try {
            connection.connect(InetAddress.getByName("localhost"), 0, this.id, false, true);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        this.aml = aml;
        connection.add(aml);
        group.join(connection,this.grupo);
        myGroup.join(connection,this.id);

    }

    @Override
    public String toString() {
        return "Grupo{" +
                ", id='" + id + '\'' +
                ", grupo='" + grupo + '\'' +
                ", primario='" + primario + '\'' +
                ", secundarios=" + secundarios +
                ", ku=" + ku +
                ", requests=" + requests +
                ", acks=" + acks +
                ", estadoRecuperado=" + estadoRecuperado +
                '}';
    }

    private JSONObject aplicaProjecao(JSONObject o, HashMap<Boolean, ArrayList<String>> p){
        ArrayList<String> f = p.get(false);
        if(f != null){
            for(String aux: f){
                try{
                    o.remove(aux);
                }
                catch(Exception e){
                    System.out.println("Excecao no retira do json");
                }
            }
        }
        ArrayList<String> t = p.get(true);
        if(t != null){
            JSONObject aux = o;
            o = new JSONObject();
            for(String a: t){
                try{
                    o.put(a,aux.get(a));
                }
                catch(Exception e){
                    System.out.println("Excecao no poe no json");
                }
            }
        }


        return o;
    }

    private boolean aplicaFiltro(Predicate<JSONObject> filtro, JSONObject jo){
        try{
            boolean res = filtro.test(jo);
            return res;
        }
        catch (Exception e){
            return false;
        }
    }

    //função que cria o filtro a partir dos vários filtros
    private Predicate<JSONObject> filtro(ArrayList<Predicate<JSONObject>> filters) {
        if(filters == null){
            return null;
        }

        Predicate<JSONObject> pred = filters.stream().reduce(Predicate::and).orElse(x -> true);
        pred = pred.negate();

        return pred;
    }

    //handler para responder ao pedido put, efetuado pelo stub
    public boolean put(PutRequest pr){

        ////se ainda n inseriu insere
        byte[] key = Config.encode(pr.key);
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

        byte[] keys = Config.encode(gr.key);
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


    public RespostaRemove remove(RemoveRequest rr){

        byte[] keys = Config.encode(rr.key);

        try {
            byte[] value = rocksDB.get(keys);

            if(value == null){
                return (new RespostaRemove(false, null, rr.key));
            }else {

                if (rr.filtros == null && rr.projecoes == null) {
                    //Vai ser um remove normal
                    rocksDB.delete(keys);

                    return (new RespostaRemove(true, null, rr.key));

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

                            return (new RespostaRemove(true, json, rr.key));
                        } else {
                            //não passou no filtro, não pode eliminar
                            return (new RespostaRemove(false, null, rr.key));
                        }
                    } else {
                        if (rr.projecoes != null) {
                            String ret = new String(value);
                            JSONObject json = new JSONObject(ret);

                            for(String s: rr.projecoes)
                                json.remove(s);

                            rocksDB.put(value, json.toString().getBytes());

                            return (new RespostaRemove(true, json, rr.key));
                        } else {
                            //são gets apenas com filtros
                            String ret = new String(value);
                            JSONObject json = new JSONObject(ret);

                            Predicate<JSONObject> filtros = this.filtro(rr.filtros);
                            if (filtros.test(json)) {
                                //Passou no filtro
                                rocksDB.delete(keys);

                                return (new RespostaRemove(true, null, rr.key));
                            } else {
                                //não passou no filtro, logo não pode eliminar
                                return (new RespostaRemove(false, null, rr.key));
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

        return (new RespostaRemove(false, null, rr.key));

    }

    //scan para todos os objectos, sem projecções
    private ResultadoScan getScan(int nrMaximo, Object ultimaChaveA, KeysUniverse ku, Predicate<JSONObject> filtros, HashMap<Boolean, ArrayList<String>> p) {
        LinkedHashMap<Object,JSONObject> docs = new LinkedHashMap<>();
        RocksIterator iterador = rocksDB.newIterator();
        int quantos = 0;
        byte[] chave = null;
        byte[] anterior = null;
        byte[] ultimaChave;
        if(ultimaChaveA == null) {
            ultimaChave = ku.min;
            iterador.seek(ultimaChave);
            chave = ultimaChave;
        }
        else{
            ultimaChave = Config.encode(ultimaChaveA);
            chave = ultimaChave;
            iterador.seek(ultimaChave);
            if(!iterador.isValid()){
                return null;
            }
            iterador.next();
        }


        while (iterador.isValid()) {
            byte[] k = iterador.key();
//            if(k <= anterior){
            if(anterior != null && Config.compareArray(k,anterior) <= 0){
                //n está neste universe
                break;
            }
            anterior = k;
            chave = k;
            String v = new String(iterador.value());
            JSONObject json = new JSONObject(v);
            boolean poe = true;

            if(filtros != null){
                poe = aplicaFiltro(filtros,json);
            }

            if(p != null){
                json = aplicaProjecao(json,p);
            }

            if(poe){
                docs.put(Config.decode(k),json);
                quantos++;
                if(quantos >= nrMaximo){
                    break;
                }
            }
            iterador.next();
        }
        return new ResultadoScan(Config.decode(chave),docs);
    }

    public ResultadoScan scan(ScanRequest sr){
        ResultadoScan docs = null; //n será muito eficiente, provavelmente por causa de andar sempre a mudar o map

        //de alguma forma faz o scan à bd, ver a melhor forma
        docs = getScan(sr.nrMaximo, sr.ultimaChave,sr.ku,filtro(sr.filtros),sr.projecoes);
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


        return docs;

    }

    public boolean updateState(UpdateMessage um){

        ////se ainda n inseriu insere
        byte[] key = Config.encode(um.key);
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


    private boolean put(Object k, JSONObject value){

        ////se ainda n inseriu insere
        byte[] key = Config.encode(k);
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

        if(!estadoAMeio) {
            estadoAMeio = true;

            if (es.requests != null) {
                requests = es.requests;
                //removeRequests = es.removeRequests;
                acks = es.acks;
                //falta remove requests
            }

        }else{

            es.requests.keySet().removeIf(a -> requests.containsKey(a));

            requests.putAll(es.requests);

            for(Object o : es.requests.values()) {
                Object key;
                if (o instanceof Remove) {
                    key = ((Remove) o).request.key;
                } else {
                    key = ((Put) o).request.key;
                }

//                if(key <= lastKey){
                if(Config.compareArray(Config.encode(key), Config.encode(lastKey)) <= 0){
                    if(o instanceof Remove){
                        remove(((Remove)o).request);
                    }
                    else{
                        put(((Put)o).request);
                    }
                }
            }
        }

        for (Map.Entry<Object, JSONObject> entry : es.valores.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }

        if(es.valores.size() != 0)
            lastKey = es.lastKey;

        estadoRecuperado = es.last;

    }


    public void pedidoEstado(PedidoEstado pe, String sender, Serializer s) {

        long quantos = 0;
        long max = 20;
        HashMap<Object,JSONObject> map = new HashMap<>();
        Object keyA = Config.decode(ku.min);
        EstadoSlave es = new EstadoSlave(pe.id,requests,acks,map,false,keyA,null);

        RocksIterator iterador = rocksDB.newIterator();
        boolean enviei = false;
        Object ultimaChave;
        if(pe.estadoAMeio){
            ultimaChave = pe.lastKey;
            byte[] seek = Config.encode(ultimaChave);
            if(seek != null) iterador.seek(seek);

            if(iterador.isValid()){
                iterador.next();
            }
        }
        else {
            ultimaChave = ku.min;
            byte[] seek = Config.encode(ultimaChave);
            if(seek != null) iterador.seek(seek);

        }

        byte[] k = null;
        while(iterador.isValid()){
            enviei = false;
            k = iterador.key();
            String v = new String(iterador.value());
            JSONObject json = new JSONObject(v);
            map.put(Config.decode(k),json);
            iterador.next();
            quantos++;
            if(quantos >= max){
                enviei = true;
                es.last = !iterador.isValid();
                es.valores = map;
                quantos = 0;
                es.lastKey = Config.decode(k);

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
            es.lastKey = (k == null ? k : Config.decode(k));

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
        HashMap<Object,JSONObject> map = new HashMap<>();
        Object keyA = Config.decode(ku.min);
        EstadoSlave es = new EstadoSlave(pe.id,requests,acks,map,false,keyA,null);

        RocksIterator iterador = rocksDB.newIterator();
        boolean enviei = false;
        Object ultimaChave;
        if(pe.estadoAMeio){
            ultimaChave = pe.lastKey;
            byte[] seek = Config.encode(ultimaChave);
            if(seek != null) iterador.seek(seek);

            if(iterador.isValid()){
                iterador.next();
            }
        }
        else {
            ultimaChave = ku.min;
            byte[] seek = Config.encode(ultimaChave);
            if(seek != null) iterador.seek(seek);

        }

        byte[] k = null;
        while(iterador.isValid()){
            enviei = false;
            k = iterador.key();
            String v = new String(iterador.value());
            JSONObject json = new JSONObject(v);
            map.put(Config.decode(k),json);
            iterador.next();
            quantos++;
            if(quantos >= max){
                enviei = true;
                es.last = !iterador.isValid();
                es.valores = map;
                quantos = 0;
                es.lastKey = Config.decode(k);

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
            es.lastKey = (k == null ? k : Config.decode(k));

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

    public void leaveGroup() {
        try {
            group.leave();
        } catch (SpreadException e) {
            e.printStackTrace();
        }
        try {
            myGroup.leave();
        } catch (SpreadException e) {
            e.printStackTrace();
        }
        try {
            connection.remove(aml);
            connection.disconnect();
        } catch (SpreadException e) {
            e.printStackTrace();
        }

    }
}
