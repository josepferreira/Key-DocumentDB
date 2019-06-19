package nodes;

import com.google.common.primitives.Longs;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import messages.*;
import org.json.JSONObject;
import spread.SpreadConnection;
import spread.SpreadException;
import spread.SpreadMessage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Time;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

class TratamentoTimeout{

    //Só vai ser um destes 3!
    public Get gets;
    public Remove remove;
    public Put put;

    public CompletableFuture<JSONObject> cfj;
    public CompletableFuture<Boolean> cfb;

    public int numeroTentativas = 0; //Se chegar a 3 por exemplo damos null ou false dependendo do CF

    public TratamentoTimeout(Get gets, CompletableFuture<JSONObject> cfj) {
        this.gets = gets;
        this.cfj = cfj;
    }

    public TratamentoTimeout(Remove remove, CompletableFuture<Boolean> cfb) {
        this.remove = remove;
        this.cfb = cfb;
    }

    public TratamentoTimeout(Put put, CompletableFuture<Boolean> cfb) {
        this.put = put;
        this.cfb = cfb;
    }
}

public class Stub {

    String endereco;
    //private final Address masterAddress = Address.from("localhost:12340");
    public ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();
    HashMap<String, TratamentoTimeout> getRequests = new HashMap<>();
    HashMap<String, TratamentoTimeout> removeRequests = new HashMap<>();
    HashMap<String, TratamentoTimeout> putRequests = new HashMap<>();
    HashMap<String, ScanIterator> scanRequests = new HashMap<>();

    private TreeMap<KeysUniverse,SlaveIdentifier> cache = new TreeMap<>();

    //Comunicação multicast
    SpreadConnection connection = new SpreadConnection();
    private HashSet<String> replys = new HashSet<>(); //Para tratar dos pedidos repetidos dos diferentes masters
    ReentrantLock lockReplys = new ReentrantLock();


    public Stub(String endereco){
        this.endereco = endereco;
        try {
            System.out.println(endereco);
            connection.connect(InetAddress.getByName("localhost"), 0, null, false, false);
        } catch (SpreadException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        this.ms = NettyMessagingService.builder().withAddress(Address.from(endereco)).build();
        this.ms.start();
        registaHandlers();
    }

    /**
     * Serve para remover o estado das respostas, para evitar que cresça infinitamente
     * @param id
     */
    private void removeReply(String id){
        //VAMOS PRECISAR DE CONTROLO DE CONCORRENCIA MUITO SIMPLES!!!!
        try {
            lockReplys.lock();
            replys.remove(id);
        }finally {
            lockReplys.unlock();
        }
    }

    /**
     * Necessário usar este runnable desta forma para conseguir passar um parametro
     * @param id
     * @return
     */
    private Runnable delete(final String id){
        Runnable ret = new Runnable() {
            @Override
            public void run() {
                removeReply(id);
            }
        };

        return ret;
    }

    /**
     * Verifica se já possui uma resposta repetida
     * Caso não exista, adiciona ao hashset e cria o schedule
     * @param id
     * @return
     */
    private boolean eRepetido(String id){

        try {

            lockReplys.lock();

            if (!this.replys.contains(id)) {
                replys.add(id);
                this.ses.schedule(delete(id), 60, TimeUnit.SECONDS);
                return false;
            }

            return true;

        }finally {
            lockReplys.unlock();
        }
    }


    private void reenviaMensagem(long key, GetRequest o){

        System.out.println("Chegei a um timeout ... !");
        System.out.println("Para ja vou pedir ao master depois podemos voltar a fazer um novo pedido ao slave!");

        KeysUniverse ku = new KeysUniverse(key, key);
        this.cache.put(ku, null);

        enviaMensagem(key, o);
        ses.schedule(timeoutJson(o.id, key, o), 10, TimeUnit.SECONDS);
    }

    private void reenviaMensagem(long key, PutRequest o){

        System.out.println("Chegei a um timeout ... !");
        System.out.println("Para ja vou pedir ao master depois podemos voltar a fazer um novo pedido ao slave!");

        KeysUniverse ku = new KeysUniverse(key, key);
        this.cache.put(ku, null);

        enviaMensagem(key, o);
        ses.schedule(timeoutJson(o.id, key, o), 10, TimeUnit.SECONDS);
    }

    private void reenviaMensagem(long key, RemoveRequest o){

        System.out.println("Chegei a um timeout ... !");
        System.out.println("Para ja vou pedir ao master depois podemos voltar a fazer um novo pedido ao slave!");

        KeysUniverse ku = new KeysUniverse(key, key);
        this.cache.put(ku, null);

        enviaMensagem(key, o);
        ses.schedule(timeoutJson(o.id, key, o), 10, TimeUnit.SECONDS);
    }

    private Runnable timeoutJson(String id, long key, Object o){
        Runnable ret = new Runnable() {
            @Override
            public void run() {
                if(o instanceof GetRequest){
                    if(getRequests.containsKey(id)) {
                        TratamentoTimeout aux = getRequests.get(id);
                        aux.numeroTentativas++;
                        if(aux.numeroTentativas != 3)
                            reenviaMensagem(key, (GetRequest) o);
                        else {
                            System.out.println("TEMOS DE ACABAR COM O CF");
                            getRequests.remove(id);
                            aux.cfj.complete(null);
                        }

                    }
                }else{
                    if(o instanceof PutRequest){
                        if(putRequests.containsKey(id)) {
                            TratamentoTimeout aux = getRequests.get(id);
                            aux.numeroTentativas++;
                            if(aux.numeroTentativas != 3)
                                reenviaMensagem(key, (PutRequest) o);
                            else {
                                System.out.println("TEMOS DE ACABAR COM O CF");
                                putRequests.remove(id);
                                aux.cfb.complete(false);
                            }
                        }
                    }
                    else{
                        if(o instanceof RemoveRequest) {
                            if (removeRequests.containsKey(id)) {
                                TratamentoTimeout aux = getRequests.get(id);
                                aux.numeroTentativas++;
                                if(aux.numeroTentativas != 3)
                                    reenviaMensagem(key, (RemoveRequest) o);
                                else {
                                    System.out.println("TEMOS DE ACABAR COM O CF");
                                    removeRequests.remove(id);
                                    aux.cfb.complete(false);
                                }
                            }
                        }
                    }
                }
            }
        };

        return ret;
    }

    private void registaHandlers(){

        /**
         *
         * Recebe uma mensagem com a resposta do get por parte do stub
         * Contem já o valor correspondete à chave (pode ser null)
         *
         */
        ms.registerHandler("getReply",(a,m) -> {
            GetReply gr = s.decode(m);

            if(gr.value == null)
                System.out.println("O valor retornado é nulo");
            else
                System.out.println("O valor é: " + gr.value.toString());
            this.getRequests.get(gr.id).gets.cf.complete(gr.value);

        },ses);

        ms.registerHandler("removeReply",(a,m) -> {
            RemoveReply rr = s.decode(m);

            System.out.println("O valor é: " + rr.sucess);
            this.removeRequests.get(rr.id).remove.cf.complete(rr.sucess);

        },ses);

        ms.registerHandler("putReply",(a,m) -> {
            PutReply pr = s.decode(m);

            System.out.println("O valor é: " + pr.success);

            //TENS DE VER ISTO AQUI!!! SE É PARA MANDAR TRUE OU NÃO ... SÓ PUS ASSIM PARA NAO DAR ERRO

            this.putRequests.get(pr.id).put.cf.complete(pr.success);

        },ses);

        ms.registerHandler("scanReply", (o,m) -> {

            SlaveScanReply ssr = s.decode(m);
            ScanIterator sc = scanRequests.get(ssr.id);


            sc.scan.registaResposta(ssr);
        }, ses);


        /**
         *
         * Recebe uma mensagem com a resposta do get por parte do master
         * Contem o endereço do slave que devemos de contactar
         *
         */

        ms.registerHandler("getMaster",(a,m) -> {
            ReplyMaster rm = s.decode(m);

            if(!eRepetido(rm.id)) {

                System.out.println("O slave que contém a minha key é: " + rm.slave.primario());
                Get g = getRequests.get(rm.id).gets;

                if (g == null) {
                    System.out.println("Deu nulo no get ... Algo errado!");
                }

                this.cache.put(rm.slave.keys,rm.slave);
                ms.sendAsync(Address.from(rm.slave.primario()), "get", s.encode(g.request));

            }

        },ses);

        ms.registerHandler("removeMaster",(a,m) -> {
            ReplyMaster rm = s.decode(m);

            if(!eRepetido(rm.id)) {

                System.out.println("O slave que contém a minha key é: " + rm.slave.primario());
                Remove r = removeRequests.get(rm.id).remove;

                if (r == null) {
                    System.out.println("Deu nulo no get ... Algo errado!");
                }

                this.cache.put(rm.slave.keys,rm.slave);
                ms.sendAsync(Address.from(rm.slave.primario()), "remove", s.encode(r.request));
            }

        },ses);

        ms.registerHandler("putMaster",(a,m) -> {
            System.out.println("Recebi uma mensagtme do master!!");
            ReplyMaster rm = s.decode(m);

            if(!eRepetido(rm.id)) {

                System.out.println("O slave que contém a minha key é: " + rm.slave.primario());

                Put p = putRequests.get(rm.id).put;
                if (p == null) {
                    //Estranho, ver este caso
                    System.out.println("Put null");
                }

                System.out.println("Por na cache");

                this.cache.put(rm.slave.keys,rm.slave);

                System.out.println("Enviar para o slave");
                ms.sendAsync(Address.from(rm.slave.primario()), "put", s.encode(p.request));
            }

        },ses);

        ms.registerHandler("scanMaster", (o,m) -> {
            ScanReply sr = s.decode(m);

            if(!eRepetido(sr.id)) {

                ScanIterator sc = scanRequests.get(sr.id);
                this.cache = sr.slaves;
                sc.scan.registaCache(this.cache);
            }

        },ses);


    }

    // ****************** GET *******************************


    private void enviaMensagem(long key, GetRequest gr){
        KeysUniverse ku = new KeysUniverse(key, key);

        SlaveIdentifier end = this.cache.get(ku);

        if(end == null) {
            SpreadMessage sm = new SpreadMessage();
            sm.setData(s.encode(gr));
            sm.addGroup("master");
            sm.setAgreed(); // ao defiirmos isto estamos a garantir ordem total, pelo q podemos ter varios stubs
            sm.setReliable();
            try {
                connection.multicast(sm);
            } catch (SpreadException e) {
                e.printStackTrace();
            }
//            ms.sendAsync(masterAddress, "get", s.encode(gr));
        }
        else
            ms.sendAsync(Address.from(end.endereco), "get", s.encode(gr));
    }



    public JSONObject get(long key){
        CompletableFuture<JSONObject> jsonCF = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        GetRequest gr = new GetRequest(requestID, key, this.endereco);
        Get g = new Get(gr, jsonCF);
        getRequests.put(requestID, new TratamentoTimeout(g, jsonCF));

        enviaMensagem(key, gr);

        JSONObject ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, gr), 10, TimeUnit.SECONDS);
            ret = jsonCF.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return ret;
    }


    public JSONObject get(long key, ArrayList<Predicate<JSONObject>> filtros){
        CompletableFuture<JSONObject> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        GetRequest gr = new GetRequest(requestID, key, this.endereco, filtros);
        Get g = new Get(gr, cf, gr.filtros, gr.projecoes);
        getRequests.put(requestID, new TratamentoTimeout(g, cf));

        enviaMensagem(key, gr);
        //ms.sendAsync(masterAddress,"get", this.s.encode(gr));

        JSONObject ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, gr), 10, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return ret;

    }

    public JSONObject get(long key, HashMap<Boolean, ArrayList<String>> projecoes){
        CompletableFuture<JSONObject> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        GetRequest gr = new GetRequest(requestID, key, this.endereco, projecoes);
        Get g = new Get(gr, cf, gr.filtros, gr.projecoes);
        getRequests.put(requestID, new TratamentoTimeout(g, cf));

        enviaMensagem(key, gr);

        JSONObject ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, gr), 10, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return ret;
    }

    public JSONObject get(long key, ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projecoes){
        CompletableFuture<JSONObject> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        GetRequest gr = new GetRequest(requestID, key, this.endereco, filtros, projecoes);
        Get g = new Get(gr, cf, gr.filtros, gr.projecoes);
        getRequests.put(requestID, new TratamentoTimeout(g, cf));

        enviaMensagem(key, gr);

        JSONObject ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, gr), 10, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return ret;

    }








    // ************************ REMOVE *****************************


    private void enviaMensagem(long key, RemoveRequest rr){
        KeysUniverse ku = new KeysUniverse(key, key);

        SlaveIdentifier end = this.cache.get(ku);

        if(end == null){
            SpreadMessage sm = new SpreadMessage();
            sm.setData(s.encode(rr));
            sm.addGroup("master");
            sm.setAgreed(); // ao defiirmos isto estamos a garantir ordem total, pelo q podemos ter varios stubs
            sm.setReliable();
            try {
                connection.multicast(sm);
            } catch (SpreadException e) {
                e.printStackTrace();
            }
        }
//            ms.sendAsync(masterAddress, "remove", s.encode(rr));
        else
            ms.sendAsync(Address.from(end.endereco), "remove", s.encode(rr));
    }


    public Boolean remove(long key){
        CompletableFuture<Boolean> jsonCF = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, this.endereco);
        Remove r = new Remove(rr, jsonCF);
        removeRequests.put(requestID, new TratamentoTimeout(r, jsonCF));

        enviaMensagem(key, rr);

        Boolean ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, rr), 10, TimeUnit.SECONDS);
            ret = jsonCF.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return ret;
    }

    public Boolean remove(long key, List<Predicate<JSONObject>> filtros){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, this.endereco, (ArrayList<Predicate<JSONObject>>) filtros, null);
        Remove r = new Remove(rr, cf, rr.filtros, rr.projecoes);
        removeRequests.put(requestID, new TratamentoTimeout(r, cf));

        enviaMensagem(key, rr);

        Boolean ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, rr), 10, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return ret;

    }

    public Boolean remove(long key, ArrayList<String> projecoes){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, this.endereco, null, projecoes);
        Remove r = new Remove(rr, cf, rr.filtros, rr.projecoes);
        removeRequests.put(requestID, new TratamentoTimeout(r, cf));

        enviaMensagem(key, rr);

        Boolean ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, rr), 10, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return ret;

    }

    public Boolean remove(long key, ArrayList<Predicate<JSONObject>> filtros, ArrayList<String> projecoes){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, this.endereco, filtros, projecoes);
        Remove r = new Remove(rr, cf, rr.filtros, rr.projecoes);
        removeRequests.put(requestID, new TratamentoTimeout(r, cf));

        enviaMensagem(key, rr);

        Boolean ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, rr), 10, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return ret;

    }



    // ************ PUT ****************

    private void enviaMensagem(long key, PutRequest pr){
        KeysUniverse ku = new KeysUniverse(key, key);
        System.out.println("TOu no envia mensagem!!!!!");
        SlaveIdentifier end = this.cache.get(ku);
        System.out.println("O endereco é nulo? " + (end==null));
        if(end == null) {
            SpreadMessage sm = new SpreadMessage();
            sm.setData(s.encode(pr));
            sm.addGroup("master");
            sm.setAgreed(); // ao defiirmos isto estamos a garantir ordem total, pelo q podemos ter varios stubs
            sm.setReliable();
            try {
                System.out.println("A enviar uma mensagem de multicast para o master!");
                connection.multicast(sm);
            } catch (SpreadException e) {
                e.printStackTrace();
            }
        }
        else {
            System.out.println("Ja tenho na cache");
            ms.sendAsync(Address.from(end.primario()), "put", s.encode(pr));
        }
    }

    public Boolean put(long key, JSONObject value){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        PutRequest pr = new PutRequest(requestID, key, this.endereco, value);
        Put p = new Put(pr, cf);
        putRequests.put(requestID, new TratamentoTimeout(p, cf));

        enviaMensagem(key, pr);

        Boolean ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, pr), 10, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return ret;

    }




    // ********************* SCAN ***********************************





    public ScanIterator scan(){
        String requestID = UUID.randomUUID().toString();
        ScanRequest sr = new ScanRequest(requestID, this.endereco,null,null,null,-1,-1);
        Scan s = new Scan(requestID,this.endereco,sr.filtros,sr.projecoes,10,ms);
        ScanIterator si = new ScanIterator(s);
        scanRequests.put(requestID,si);

        SpreadMessage sm = new SpreadMessage();
        sm.setData(this.s.encode(sr));
        sm.addGroup("master");
        sm.setAgreed(); // ao defiirmos isto estamos a garantir ordem total, pelo q podemos ter varios stubs
        sm.setReliable();
        try {
            connection.multicast(sm);
        } catch (SpreadException e) {
            e.printStackTrace();
        }

        return si;
    }

    /*public CompletableFuture<ArrayList<CompletableFuture<LinkedHashMap<Long,JSONObject>>>> scan(ArrayList<Predicate<JSONObject>> filtros){
        CompletableFuture<Void> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        ScanRequest sr = new ScanRequest(requestID,filtros,null, null);
        Scan s = new Scan(requestID,cf,sr.filtros,sr.projeções);
        scanRequests.put(requestID,s);

        ms.sendAsync(masterAddress,"scan", this.s.encode(requestID));

        CompletableFuture<ArrayList<CompletableFuture<LinkedHashMap<Long,JSONObject>>>> ncf =  cf.thenCompose(a -> {return s.cfs;});
        return ncf;

    }

    public CompletableFuture<ArrayList<CompletableFuture<LinkedHashMap<Long,JSONObject>>>> scan(HashMap<Boolean, ArrayList<String>> projecoes){
        CompletableFuture<Void> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        ScanRequest sr = new ScanRequest(requestID,null,projecoes, null);
        Scan s = new Scan(requestID,cf,sr.filtros,sr.projeções);
        scanRequests.put(requestID,s);

        ms.sendAsync(masterAddress,"scan", this.s.encode(requestID));

        CompletableFuture<ArrayList<CompletableFuture<LinkedHashMap<Long,JSONObject>>>> ncf =  cf.thenCompose(a -> {return s.cfs;});
        return ncf;
    }

    public CompletableFuture<ArrayList<CompletableFuture<LinkedHashMap<Long,JSONObject>>>> scan(ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projecoes){
        CompletableFuture<Void> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        ScanRequest sr = new ScanRequest(requestID,filtros,projecoes, null);
        Scan s = new Scan(requestID,cf,sr.filtros,sr.projeções);
        scanRequests.put(requestID,s);

        ms.sendAsync(masterAddress,"scan", this.s.encode(requestID));

        CompletableFuture<ArrayList<CompletableFuture<LinkedHashMap<Long,JSONObject>>>> ncf =  cf.thenCompose(a -> {return s.cfs;});
        return ncf;

    }*/

    public static void main(String[] args) {

        String endereco = "localhost:12346";

        Stub s = new Stub(endereco);

        //s.get(10001);
//        s.get(150);
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        s.remove(150);
//        s.get(150);
//
//        while(true){
//            try {
//                Thread.sleep(1000000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
        /*JSONObject jo = new JSONObject();
        for(int i = 120; i < 125; i++){
            jo.put("obj",i);
            boolean res =  s.put(i,jo);
            System.out.println("Put feito: " + i + "! RES: " + res);
        }
        System.out.println("Puts feitos");*/
        System.out.println("Vou fazer remove");
        System.out.println("remove feito: " + s.remove(121));
//        try {
//            System.out.println(s.get(250).get());
//            System.out.println(s.remove(268).get());
//            System.out.println(s.get(268).get());
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
        System.out.println("SCAN");
        ScanIterator si = s.scan();

        while(si.hasNext()){

            Map.Entry<Long, JSONObject> a = si.next();
            System.out.println(a);
        }


        System.out.println("Terminou o scan");

    }

}
