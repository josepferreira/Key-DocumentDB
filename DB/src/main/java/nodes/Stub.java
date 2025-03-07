package nodes;

import Configuration.Config;
import Configuration.KeysUniverse;
import Configuration.SerializerProtocol;
import Operations.*;
import Support.RoundRobin;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import messages.Operation.*;
import org.json.JSONObject;
import spread.SpreadConnection;
import spread.SpreadException;
import spread.SpreadMessage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

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

    private TreeMap<KeysUniverse, RoundRobin> cache = new TreeMap<>();

    //Comunicação multicast
    SpreadConnection connection = new SpreadConnection();
    private HashSet<String> replys = new HashSet<>(); //Para tratar dos pedidos repetidos dos diferentes masters
    ReentrantLock lockReplys = new ReentrantLock();


    public Stub(){
        try {
            System.out.println("Vou conectar ao spread");
            connection.connect(InetAddress.getByName("localhost"), 0, null, false, false);
        } catch (SpreadException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        boolean started = false;
        int porta = Config.getPorta();
        this.endereco = Config.hostAtomix + ":" + porta;
        while(!started){
            try{
                ms = NettyMessagingService.builder().withAddress(Address.from(this.endereco)).build();
                ms.start().get();
                started = true;
            }
            catch(Exception e){
                System.out.println("Porta em uso: " + porta);
                porta++;
                this.endereco = Config.hostAtomix + ":" + porta;
            }
        }
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


    private void reenviaMensagem(Object key, GetRequest o){

        System.out.println("Chegei a um timeout ... !");

        KeysUniverse ku = new KeysUniverse(key, key);
        this.cache.put(ku, null);

        enviaMensagem(key, o);
        ses.schedule(timeoutJson(o.id, key, o), 15, TimeUnit.SECONDS);
    }

    private void reenviaMensagem(Object key, PutRequest o){

        System.out.println("Chegei a um timeout ... !");

        KeysUniverse ku = new KeysUniverse(key, key);
        this.cache.put(ku, null);

        enviaMensagem(key, o);
        ses.schedule(timeoutJson(o.id, key, o), 15, TimeUnit.SECONDS);
    }

    private void reenviaMensagem(Object key, RemoveRequest o){

        System.out.println("Chegei a um timeout ... !");

        KeysUniverse ku = new KeysUniverse(key, key);
        this.cache.put(ku, null);

        enviaMensagem(key, o);
        ses.schedule(timeoutJson(o.id, key, o), 15, TimeUnit.SECONDS);
    }

    private Runnable timeoutJson(String id, Object key, Object o){
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
                            getRequests.remove(id);
                            aux.cfj.completeExceptionally(new Exception("Erro, limite de 3 tentativas de timeout!"));
                        }

                    }
                }else{
                    if(o instanceof PutRequest){
                        if(putRequests.containsKey(id)) {
                            TratamentoTimeout aux = putRequests.get(id);
                            aux.numeroTentativas++;
                            if(aux.numeroTentativas != 3)
                                reenviaMensagem(key, (PutRequest) o);
                            else {
                                putRequests.remove(id);
                                aux.cfb.completeExceptionally(new Exception("Erro, limite de 3 tentativas de timeout!"));
                            }
                        }
                    }
                    else{
                        if(o instanceof RemoveRequest) {
                            if (removeRequests.containsKey(id)) {
                                TratamentoTimeout aux = removeRequests.get(id);
                                aux.numeroTentativas++;
                                if(aux.numeroTentativas != 3)
                                    reenviaMensagem(key, (RemoveRequest) o);
                                else {
                                    removeRequests.remove(id);
                                    aux.cfb.completeExceptionally(new Exception("Erro, limite de 3 tentativas de timeout!"));

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

//            if(gr.value == null)
//                System.out.println("O valor retornado é nulo");
//            else
//                System.out.println("O valor é: " + gr.value.toString());
            this.getRequests.get(gr.id).gets.cf.complete(gr.value);
            this.getRequests.remove(gr.id);

        },ses);

        ms.registerHandler("removeReply",(a,m) -> {
            RemoveReply rr = s.decode(m);

//            System.out.println("O valor é: " + rr.sucess);
            this.removeRequests.get(rr.id).remove.cf.complete(rr.sucess);
            this.removeRequests.remove(rr.id);

        },ses);

        ms.registerHandler("putReply",(a,m) -> {
            PutReply pr = s.decode(m);

//            System.out.println("O valor é: " + pr.success);

            //TENS DE VER ISTO AQUI!!! SE É PARA MANDAR TRUE OU NÃO ... SÓ PUS ASSIM PARA NAO DAR ERRO

            this.putRequests.get(pr.id).put.cf.complete(pr.success);
            this.putRequests.remove(pr.id);

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

//                System.out.println("O slave que contém a minha key é: " + rm.slave.primario());
                Get g = getRequests.get(rm.id).gets;

                if (g == null) {
//                    System.out.println("Deu nulo no get ... Algo errado!");
                }

                this.cache.put(rm.slave.keys,new RoundRobin(rm.slave));
                ms.sendAsync(Address.from(rm.slave.primario()), "get", s.encode(g.request));

            }

        },ses);

        ms.registerHandler("removeMaster",(a,m) -> {
            ReplyMaster rm = s.decode(m);

            if(!eRepetido(rm.id)) {

//                System.out.println("O slave que contém a minha key é: " + rm.slave.primario());
                Remove r = removeRequests.get(rm.id).remove;

                if (r == null) {
//                    System.out.println("Deu nulo no get ... Algo errado!");
                }

                this.cache.put(rm.slave.keys,new RoundRobin(rm.slave));
                ms.sendAsync(Address.from(rm.slave.primario()), "remove", s.encode(r.request));
            }

        },ses);

        ms.registerHandler("putMaster",(a,m) -> {
            ReplyMaster rm = s.decode(m);

            if(!eRepetido(rm.id)) {


                Put p = putRequests.get(rm.id).put;
                if (p == null) {
                    //Estranho, ver este caso
//                    System.out.println("Put null");
                }

//                System.out.println("Por na cache");

                this.cache.put(rm.slave.keys,new RoundRobin(rm.slave));

//                System.out.println("Enviar para o slave");
                ms.sendAsync(Address.from(rm.slave.primario()), "put", s.encode(p.request));
            }

        },ses);

        ms.registerHandler("scanMaster", (o,m) -> {
            ScanReply sr = s.decode(m);

            if(!eRepetido(sr.id)) {

                ScanIterator sc = scanRequests.get(sr.id);
                this.cache.clear();
                for(Map.Entry<KeysUniverse,SlaveIdentifier> entry : sr.slaves.entrySet()){
                    this.cache.put(entry.getKey(), new RoundRobin(entry.getValue()));
                }
                sc.scan.registaCache(this.cache);
            }

        },ses);


    }

    // ****************** GET *******************************


    private void enviaMensagem(Object key, GetRequest gr){
        KeysUniverse ku = new KeysUniverse(key, key);

        RoundRobin end = this.cache.get(ku);

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
            ms.sendAsync(Address.from(end.proximo()), "get", s.encode(gr));
    }



    public JSONObject get(Object key) throws Exception{
        CompletableFuture<JSONObject> jsonCF = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        GetRequest gr = new GetRequest(requestID, key, this.endereco);
        Get g = new Get(gr, jsonCF);
        getRequests.put(requestID, new TratamentoTimeout(g, jsonCF));

        enviaMensagem(key, gr);

        JSONObject ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, gr), 15, TimeUnit.SECONDS);
            ret = jsonCF.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
//            System.out.println(("ERRO TIMEOUT"));
//            e.printStackTrace();
            throw new Exception(e.getMessage());
        }


        return ret;
    }


    public JSONObject get(Object key, ArrayList<Predicate<JSONObject>> filtros) throws Exception {
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
            ses.schedule(timeoutJson(requestID, key, gr), 15, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
//            System.out.println(("ERRO TIMEOUT"));
//            e.printStackTrace();
            throw new Exception(e.getMessage());
        }


        return ret;

    }

    public JSONObject get(Object key, HashMap<Boolean, ArrayList<String>> projecoes) throws Exception {
        CompletableFuture<JSONObject> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        GetRequest gr = new GetRequest(requestID, key, this.endereco, projecoes);
        Get g = new Get(gr, cf, gr.filtros, gr.projecoes);
        getRequests.put(requestID, new TratamentoTimeout(g, cf));

        enviaMensagem(key, gr);

        JSONObject ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, gr), 15, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
//            System.out.println(("ERRO TIMEOUT"));
//            e.printStackTrace();
            throw new Exception(e.getMessage());
        }


        return ret;
    }

    public JSONObject get(Object key, ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projecoes) throws Exception {
        CompletableFuture<JSONObject> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        GetRequest gr = new GetRequest(requestID, key, this.endereco, filtros, projecoes);
        Get g = new Get(gr, cf, gr.filtros, gr.projecoes);
        getRequests.put(requestID, new TratamentoTimeout(g, cf));

        enviaMensagem(key, gr);

        JSONObject ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, gr), 15, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
//            System.out.println(("ERRO TIMEOUT"));
//            e.printStackTrace();
            throw new Exception(e.getMessage());
        }


        return ret;

    }








    // ************************ REMOVE *****************************


    private void enviaMensagem(Object key, RemoveRequest rr){
        KeysUniverse ku = new KeysUniverse(key, key);

        RoundRobin end = this.cache.get(ku);

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
            ms.sendAsync(Address.from(end.si.primario()), "remove", s.encode(rr));
    }


    public Boolean remove(Object key) throws Exception {
        CompletableFuture<Boolean> jsonCF = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, this.endereco);
        Remove r = new Remove(rr, jsonCF);
        removeRequests.put(requestID, new TratamentoTimeout(r, jsonCF));

        enviaMensagem(key, rr);

        Boolean ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, rr), 15, TimeUnit.SECONDS);
            ret = jsonCF.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
//            System.out.println(("ERRO TIMEOUT"));
//            e.printStackTrace();
            throw new Exception(e.getMessage());
        }


        return ret;
    }

    public Boolean remove(Object key, List<Predicate<JSONObject>> filtros) throws Exception {
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, this.endereco, (ArrayList<Predicate<JSONObject>>) filtros, null);
        Remove r = new Remove(rr, cf, rr.filtros, rr.projecoes);
        removeRequests.put(requestID, new TratamentoTimeout(r, cf));

        enviaMensagem(key, rr);

        Boolean ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, rr), 15, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
//            System.out.println(("ERRO TIMEOUT"));
//            e.printStackTrace();
            throw new Exception(e.getMessage());
        }


        return ret;

    }

    public Boolean remove(Object key, ArrayList<String> projecoes) throws Exception {
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, this.endereco, null, projecoes);
        Remove r = new Remove(rr, cf, rr.filtros, rr.projecoes);
        removeRequests.put(requestID, new TratamentoTimeout(r, cf));

        enviaMensagem(key, rr);

        Boolean ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, rr), 15, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
//            System.out.println(("ERRO TIMEOUT"));
//            e.printStackTrace();
            throw new Exception(e.getMessage());
        }


        return ret;

    }

    public Boolean remove(Object key, ArrayList<Predicate<JSONObject>> filtros, ArrayList<String> projecoes) throws Exception {
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, this.endereco, filtros, projecoes);
        Remove r = new Remove(rr, cf, rr.filtros, rr.projecoes);
        removeRequests.put(requestID, new TratamentoTimeout(r, cf));

        enviaMensagem(key, rr);

        Boolean ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, rr), 15, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
//            e.printStackTrace();
            throw new Exception(e.getMessage());
        }

        return ret;

    }



    // ************ PUT ****************

    private void enviaMensagem(Object key, PutRequest pr){
        KeysUniverse ku = new KeysUniverse(key, key);
        RoundRobin end = this.cache.get(ku);
        if(end == null) {
            SpreadMessage sm = new SpreadMessage();
            sm.setData(s.encode(pr));
            sm.addGroup("master");
            sm.setAgreed(); // ao defiirmos isto estamos a garantir ordem total, pelo q podemos ter varios stubs
            sm.setReliable();
            try {
                connection.multicast(sm);
            } catch (SpreadException e) {
                e.printStackTrace();
            }
        }
        else {
            ms.sendAsync(Address.from(end.si.primario()), "put", s.encode(pr));
        }
    }

    public Boolean put(Object key, JSONObject value) throws Exception {
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        PutRequest pr = new PutRequest(requestID, key, this.endereco, value);
        Put p = new Put(pr, cf);
        putRequests.put(requestID, new TratamentoTimeout(p, cf));

        enviaMensagem(key, pr);

        Boolean ret = null;
        try {
            //lançar ses para fazer o timeout
            ses.schedule(timeoutJson(requestID, key, pr), 15, TimeUnit.SECONDS);
            ret = cf.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
//            e.printStackTrace();
            throw new Exception(e.getMessage());
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

    public ScanIterator scan(ArrayList<Predicate<JSONObject>> filtro){
        String requestID = UUID.randomUUID().toString();
        ScanRequest sr = new ScanRequest(requestID, this.endereco,filtro,null,null,-1,-1);
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

    public ScanIterator scan(HashMap<Boolean, ArrayList<String>> projecoes){
        String requestID = UUID.randomUUID().toString();
        ScanRequest sr = new ScanRequest(requestID, this.endereco,null,projecoes,null,-1,-1);
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

    public ScanIterator scan(ArrayList<Predicate<JSONObject>> filtro, HashMap<Boolean, ArrayList<String>> projecoes){
        String requestID = UUID.randomUUID().toString();
        ScanRequest sr = new ScanRequest(requestID, this.endereco,filtro,projecoes,null,-1,-1);
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

    private Predicate<JSONObject> filtro(ArrayList<Predicate<JSONObject>> filters) {
        if(filters == null){
            return null;
        }

        Predicate<JSONObject> pred = filters.stream().reduce(Predicate::and).orElse(x -> true);
        pred = pred.negate();

        return pred;
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


        if(args.length == 0){
            System.out.println("Coloque um argumento!");
            return;
        }

        Stub s = new Stub();

        System.out.println("\n\n");
        System.out.println("Stub criado");

        if(args[0].equals("0")){
            int i = 45;
            JSONObject jo = new JSONObject();
            jo.put("obj",i);
            jo.put("campo", "json");
            boolean res = false;
            try {
                res = s.put(i,jo);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("Put feito: " + i + "! RES: " + res);

            try {
                System.out.println("GET " + i + ": " + s.get(i));
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                System.out.println("GET " + (i+1) + ": " + s.get(i+1));
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        else if(args[0].equals("1")){

            JSONObject jo = new JSONObject();
            for(int i = 320; i < 342; i++){
                jo.put("obj",i);
                boolean res = false;
                try {
                    res = s.put(i,jo);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("Put feito: " + i + "! RES: " + res);
            }

            System.out.println("SCAN:");
            ScanIterator si = s.scan();

            while (si.hasNext()) {

                Map.Entry<Object, JSONObject> a = si.next();
                System.out.println(a);
            }


            System.out.println("Terminou o scan");


        }
        else if(args[0].equals("2")){

            int i = 135;
            JSONObject jo = new JSONObject();
            jo.put("obj",i);
            jo.put("campo", "json");
            boolean res = false;
            try {
                res = s.put(i,jo);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("Put feito: " + i + "! RES: " + res);

            try {
                System.out.println("GET " +  i + ": " + s.get(i));
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                HashMap<Boolean,ArrayList<String>> proj = new HashMap<>();
                ArrayList<String> m = new ArrayList<>();
                m.add("obj");
                proj.put(true,m);
                System.out.println("GET com projecao " + i + ": " + s.get(i,proj));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {
            int i = 60;
            JSONObject jo = new JSONObject();
            jo.put("obj",i);
            jo.put("campo", "json");
            boolean res = false;
            try {
                res = s.put(i,jo);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("Put feito: " + i + "! RES: " + res);

            try {
                System.out.println("GET " +  i + ": " + s.get(i));
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                boolean aux = s.remove(i);
                System.out.println("Remove feito: " + aux);
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                System.out.println("GET " +  i + ": " + s.get(i));
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        System.out.println("Terminaram as operações!");

    }

}
