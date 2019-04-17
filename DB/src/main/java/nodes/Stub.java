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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

public class Stub {

    String endereco;
    private final Address masterAddress = Address.from("localhost:12340");
    ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();
    HashMap<String, Get> getRequests = new HashMap<>();
    HashMap<String, Remove> removeRequests = new HashMap<>();
    HashMap<String, Put> putRequests = new HashMap<>();
    HashMap<String, Scan> scanRequests = new HashMap<>();

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
            GetReply gr = s.decode(m);

            System.out.println("O valor é: " + gr.value.toString());

            //TENS DE VER ISTO AQUI!!! SE É PARA MANDAR TRUE OU NÃO ... SÓ PUS ASSIM PARA NAO DAR ERRO

            this.putRequests.get(gr.id).cf.complete(true);

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

            ms.sendAsync(Address.from(rm.endereco), "get", s.encode(g.request));

        },ses);

        ms.registerHandler("removeMaster",(a,m) -> {
            ReplyMaster rm = s.decode(m);

            System.out.println("O slave que contém a minha key é: " + rm.endereco);
            Remove r = removeRequests.get(rm.id);

            if(r == null){
                System.out.println("Deu nulo no get ... Algo errado!");
            }

            ms.sendAsync(Address.from(rm.endereco), "remove", s.encode(r.request));

        },ses);

        ms.registerHandler("putMaster",(a,m) -> {
            ReplyMaster rm = s.decode(m);

            System.out.println("O slave que contém a minha key é: " + rm.endereco);

            Put p = putRequests.get(rm.id);
            if(p == null){
                //Estranho, ver este caso
            }

            ms.sendAsync(Address.from(rm.endereco), "put", s.encode(p.request));

        },ses);

        ms.registerHandler("scanMaster", (o,m) -> {
            ScanReply sr = s.decode(m);

            Scan sc = scanRequests.get(sr.id);
            sc.adicionaSlaves(sr.slaves);

            Collection<CompletableFuture<Void>> aux = sc.cfsSlaves.values();
            CompletableFuture.allOf(aux.toArray(new CompletableFuture[aux.size()])).thenAccept( a -> {

                LinkedHashMap<Long,JSONObject> res = new LinkedHashMap<>();
                for(SlaveIdentifier si : sc.slaves.values()){
                    res.putAll(sc.respostas.get(si));
                }

                sc.cf.complete(res);
            });
            ScanRequest srq = new ScanRequest(sc.id,sc.filtros,sc.projeções);
            for(SlaveIdentifier si : sc.slaves.values()){
                ms.sendAsync(Address.from(si.endereco), "scan", s.encode(srq));
            }

        },ses);

        ms.registerHandler("scanReply", (o,m) -> {

            SlaveScanReply ssr = s.decode(m);

            Scan sc = scanRequests.get(ssr.id);

            SlaveIdentifier si = new SlaveIdentifier(o.toString(),null);
            sc.respostas.put(si,ssr.docs);
            sc.cfsSlaves.get(o.toString()).complete(null);


        }, ses);


    }

    // ****************** GET *******************************





    public CompletableFuture<JSONObject> get(long key){
        CompletableFuture<JSONObject> jsonCF = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        GetRequest gr = new GetRequest(requestID, key);
        Get g = new Get(gr, jsonCF);
        getRequests.put(requestID, g);

        ms.sendAsync(masterAddress, "get", s.encode(gr));

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

        ms.sendAsync(masterAddress,"get", this.s.encode(gr));

        return cf;

    }

    public CompletableFuture< JSONObject> get(long key, ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projecoes){
        CompletableFuture<JSONObject> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        GetRequest gr = new GetRequest(requestID, key, filtros, projecoes);
        Get g = new Get(gr, cf, gr.filtros, gr.projecoes);
        getRequests.put(requestID, g);

        ms.sendAsync(masterAddress,"get", this.s.encode(gr));

        return cf;

    }








    // ************************ REMOVE *****************************





    public CompletableFuture<Boolean> remove(long key){
        CompletableFuture<Boolean> jsonCF = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key);
        Remove r = new Remove(rr, jsonCF);
        removeRequests.put(requestID, r);

        ms.sendAsync(masterAddress, "remove", s.encode(rr));

        return jsonCF;
    }

    public CompletableFuture<Boolean> remove(long key, List<Predicate<JSONObject>> filtros){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, (ArrayList<Predicate<JSONObject>>) filtros, null);
        Remove r = new Remove(rr, cf, rr.filtros, rr.projecoes);
        removeRequests.put(requestID, r);

        ms.sendAsync(masterAddress,"get", this.s.encode(rr));

        return cf;

    }

    public CompletableFuture<Boolean>remove(long key, ArrayList<String> projecoes){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, null, projecoes);
        Remove r = new Remove(rr, cf, rr.filtros, rr.projecoes);
        removeRequests.put(requestID, r);

        ms.sendAsync(masterAddress,"get", this.s.encode(rr));

        return cf;

    }

    public CompletableFuture<Boolean> remove(long key, ArrayList<Predicate<JSONObject>> filtros, ArrayList<String> projecoes){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        RemoveRequest rr = new RemoveRequest(requestID, key, filtros, projecoes);
        Remove r = new Remove(rr, cf, rr.filtros, rr.projecoes);
        removeRequests.put(requestID, r);

        ms.sendAsync(masterAddress,"get", this.s.encode(rr));

        return cf;

    }



    // ************ PUT ****************

    public CompletableFuture<Boolean> put(long key, JSONObject value){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        PutRequest pr = new PutRequest(requestID,key,value);
        Put p = new Put(pr, cf);
        putRequests.put(requestID, p);

        ms.sendAsync(masterAddress,"put", s.encode(pr));

        return cf;

    }




    // ********************* SCAN ***********************************





    public CompletableFuture<LinkedHashMap<Long,JSONObject>> scan(){
        CompletableFuture<LinkedHashMap<Long,JSONObject>> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        ScanRequest sr = new ScanRequest(requestID,null,null);
        Scan s = new Scan(requestID,cf,sr.filtros,sr.projeções);
        scanRequests.put(requestID,s);

        ms.sendAsync(masterAddress,"scan", this.s.encode(requestID));

        return cf;


    }

    public CompletableFuture<LinkedHashMap<Long,JSONObject>> scan(ArrayList<Predicate<JSONObject>> filtros){
        CompletableFuture<LinkedHashMap<Long,JSONObject>> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        ScanRequest sr = new ScanRequest(requestID,filtros,null);
        Scan s = new Scan(requestID,cf,sr.filtros,sr.projeções);
        scanRequests.put(requestID,s);

        ms.sendAsync(masterAddress,"scan", this.s.encode(requestID));

        return cf;

    }

    public CompletableFuture<LinkedHashMap<Long,JSONObject>> scan(HashMap<Boolean, ArrayList<String>> projecoes){
        CompletableFuture<LinkedHashMap<Long,JSONObject>> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        ScanRequest sr = new ScanRequest(requestID,null,projecoes);
        Scan s = new Scan(requestID,cf,sr.filtros,sr.projeções);
        scanRequests.put(requestID,s);

        ms.sendAsync(masterAddress,"scan", this.s.encode(requestID));

        return cf;

    }

    public CompletableFuture<LinkedHashMap<Long,JSONObject>> scan(ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projecoes){
        CompletableFuture<LinkedHashMap<Long,JSONObject>> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        ScanRequest sr = new ScanRequest(requestID,filtros,projecoes);
        Scan s = new Scan(requestID,cf,sr.filtros,sr.projeções);
        scanRequests.put(requestID,s);

        ms.sendAsync(masterAddress,"scan", this.s.encode(requestID));

        return cf;

    }

    public static void main(String[] args) {

        String endereco = "localhost:12346";

        Stub s = new Stub(endereco);

        //s.get(10001);
        s.get(150);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        s.remove(150);
        s.get(150);

        while(true){
            try {
                Thread.sleep(1000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
