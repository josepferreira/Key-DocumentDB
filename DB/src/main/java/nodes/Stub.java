package nodes;

import com.google.common.primitives.Longs;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import messages.*;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Stub {

    String endereco;
    private final Address masterAddress = Address.from("localhost:12340");
    public ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();
    HashMap<String, Get> getRequests = new HashMap<>();
    HashMap<String, Remove> removeRequests = new HashMap<>();
    HashMap<String, Put> putRequests = new HashMap<>();
    HashMap<String, ScanIterator> scanRequests = new HashMap<>();

    private TreeMap<KeysUniverse,SlaveIdentifier> cache = new TreeMap<>();

    public Stub(String endereco){
        this.endereco = endereco;
        this.ms = NettyMessagingService.builder().withAddress(Address.from(endereco)).build();

        this.ms.start();
        registaHandlers();
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
            this.getRequests.get(gr.id).cf.complete(gr.value);

        },ses);

        ms.registerHandler("removeReply",(a,m) -> {
            RemoveReply rr = s.decode(m);

            System.out.println("O valor é: " + rr.sucess);
            this.removeRequests.get(rr.id).cf.complete(rr.sucess);

        },ses);

        ms.registerHandler("putReply",(a,m) -> {
            PutReply pr = s.decode(m);

            System.out.println("O valor é: " + pr.success);

            //TENS DE VER ISTO AQUI!!! SE É PARA MANDAR TRUE OU NÃO ... SÓ PUS ASSIM PARA NAO DAR ERRO

            this.putRequests.get(pr.id).cf.complete(pr.success);

        },ses);


        /**
         *
         * Recebe uma mensagem com a resposta do get por parte do master
         * Contem o endereço do slave que devemos de contactar
         *
         */

        ms.registerHandler("getMaster",(a,m) -> {
            ReplyMaster rm = s.decode(m);

            System.out.println("O slave que contém a minha key é: " + rm.endereco);
            Get g = getRequests.get(rm.id);

            if(g == null){
                System.out.println("Deu nulo no get ... Algo errado!");
            }

            this.cache.put(rm.keys, new SlaveIdentifier(rm.endereco,rm.keys));
            ms.sendAsync(Address.from(rm.endereco), "get", s.encode(g.request));

        },ses);

        ms.registerHandler("removeMaster",(a,m) -> {
            ReplyMaster rm = s.decode(m);

            System.out.println("O slave que contém a minha key é: " + rm.endereco);
            Remove r = removeRequests.get(rm.id);

            if(r == null){
                System.out.println("Deu nulo no get ... Algo errado!");
            }

            this.cache.put(rm.keys, new SlaveIdentifier(rm.endereco,rm.keys));
            ms.sendAsync(Address.from(rm.endereco), "remove", s.encode(r.request));

        },ses);

        ms.registerHandler("putMaster",(a,m) -> {
            System.out.println("Recebi uma mensagtme do master!!");
            ReplyMaster rm = s.decode(m);

            System.out.println("O slave que contém a minha key é: " + rm.endereco);

            Put p = putRequests.get(rm.id);
            if(p == null){
                //Estranho, ver este caso
                System.out.println("Put null");
            }

            System.out.println("Por na cache");

            this.cache.put(rm.keys, new SlaveIdentifier(rm.endereco,rm.keys));

            System.out.println("Enviar para o slave");
            ms.sendAsync(Address.from(rm.endereco), "put", s.encode(p.request));

        },ses);

        ms.registerHandler("scanMaster", (o,m) -> {
            ScanReply sr = s.decode(m);
            ScanIterator sc = scanRequests.get(sr.id);
            this.cache = sr.slaves;
            sc.scan.registaCache(this.cache);

        },ses);

        ms.registerHandler("scanReply", (o,m) -> {

            SlaveScanReply ssr = s.decode(m);
            ScanIterator sc = scanRequests.get(ssr.id);


            sc.scan.registaResposta(ssr);
        }, ses);


    }

    // ****************** GET *******************************


    private void enviaMensagem(long key, GetRequest gr){
        KeysUniverse ku = new KeysUniverse(key, key);

        SlaveIdentifier end = this.cache.get(ku);

        if(end == null)
            ms.sendAsync(masterAddress, "get", s.encode(gr));
        else
            ms.sendAsync(Address.from(end.endereco), "get", s.encode(gr));
    }



    public CompletableFuture<JSONObject> get(long key){
        CompletableFuture<JSONObject> jsonCF = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        GetRequest gr = new GetRequest(requestID, key);
        Get g = new Get(gr, jsonCF);
        getRequests.put(requestID, g);

        enviaMensagem(key, gr);

        return jsonCF;
    }


    public CompletableFuture<JSONObject> get(long key, ArrayList<Predicate<JSONObject>> filtros){
        CompletableFuture<JSONObject> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        GetRequest gr = new GetRequest(requestID, key, filtros);
        Get g = new Get(gr, cf, gr.filtros, gr.projecoes);
        getRequests.put(requestID, g);

        ms.sendAsync(masterAddress,"get", this.s.encode(gr));

        return cf;

    }

    public CompletableFuture<JSONObject> get(long key, HashMap<Boolean, ArrayList<String>> projecoes){
        CompletableFuture<JSONObject> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        GetRequest gr = new GetRequest(requestID, key, projecoes);
        Get g = new Get(gr, cf, gr.filtros, gr.projecoes);
        getRequests.put(requestID, g);

        enviaMensagem(key, gr);
        return cf;

    }

    public CompletableFuture< JSONObject> get(long key, ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projecoes){
        CompletableFuture<JSONObject> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        GetRequest gr = new GetRequest(requestID, key, filtros, projecoes);
        Get g = new Get(gr, cf, gr.filtros, gr.projecoes);
        getRequests.put(requestID, g);

        enviaMensagem(key, gr);
        return cf;

    }








    // ************************ REMOVE *****************************


    private void enviaMensagem(long key, RemoveRequest rr){
        KeysUniverse ku = new KeysUniverse(key, key);

        SlaveIdentifier end = this.cache.get(ku);

        if(end == null)
            ms.sendAsync(masterAddress, "remove", s.encode(rr));
        else
            ms.sendAsync(Address.from(end.endereco), "remove", s.encode(rr));
    }


    public CompletableFuture<Boolean> remove(long key){
        CompletableFuture<Boolean> jsonCF = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key);
        Remove r = new Remove(rr, jsonCF);
        removeRequests.put(requestID, r);

        enviaMensagem(key, rr);
        return jsonCF;
    }

    public CompletableFuture<Boolean> remove(long key, List<Predicate<JSONObject>> filtros){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, (ArrayList<Predicate<JSONObject>>) filtros, null);
        Remove r = new Remove(rr, cf, rr.filtros, rr.projecoes);
        removeRequests.put(requestID, r);

        enviaMensagem(key, rr);
        return cf;

    }

    public CompletableFuture<Boolean>remove(long key, ArrayList<String> projecoes){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, null, projecoes);
        Remove r = new Remove(rr, cf, rr.filtros, rr.projecoes);
        removeRequests.put(requestID, r);

        enviaMensagem(key, rr);
        return cf;

    }

    public CompletableFuture<Boolean> remove(long key, ArrayList<Predicate<JSONObject>> filtros, ArrayList<String> projecoes){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, filtros, projecoes);
        Remove r = new Remove(rr, cf, rr.filtros, rr.projecoes);
        removeRequests.put(requestID, r);

        enviaMensagem(key, rr);
        return cf;

    }



    // ************ PUT ****************

    private void enviaMensagem(long key, PutRequest pr){
        KeysUniverse ku = new KeysUniverse(key, key);
        System.out.println("TOu no envia mensagem!!!!!");
        SlaveIdentifier end = this.cache.get(ku);
        System.out.println("O endereco é nulo? " + (end==null));
        if(end == null) {
            System.out.println("Ainda nao tenho na cache");
            ms.sendAsync(masterAddress, "put", s.encode(pr));
        }
        else {
            System.out.println("Ja tenho na cache");
            ms.sendAsync(Address.from(end.endereco), "put", s.encode(pr));
        }
    }

    public CompletableFuture<Boolean> put(long key, JSONObject value){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        PutRequest pr = new PutRequest(requestID,key,value);
        Put p = new Put(pr, cf);
        putRequests.put(requestID, p);

        enviaMensagem(key, pr);

        return cf;

    }




    // ********************* SCAN ***********************************





    public ScanIterator scan(){
        String requestID = UUID.randomUUID().toString();
        ScanRequest sr = new ScanRequest(requestID,null,null,null,-1,-1);
        Scan s = new Scan(requestID,sr.filtros,sr.projecoes,10,ms);
        ScanIterator si = new ScanIterator(s);
        scanRequests.put(requestID,si);

        ms.sendAsync(masterAddress,"scan", this.s.encode(requestID));

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
        JSONObject jo = new JSONObject();
        for(int i = 120; i < 125; i++){
            jo.put("obj",i);
            try {
                s.put(i,jo).get();
                System.out.println("Put feito: " + i);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Puts feitos");
//        try {
//            System.out.println(s.get(250).get());
//            System.out.println(s.remove(268).get());
//            System.out.println(s.get(268).get());
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
        ScanIterator si = s.scan();

        while(si.hasNext()){

            Map.Entry<Long, JSONObject> a = si.next();
            System.out.println(a);
        }

        System.out.println("Terminou o scan");

    }

}
