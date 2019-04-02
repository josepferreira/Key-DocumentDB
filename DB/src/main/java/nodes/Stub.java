package nodes;

import com.google.common.primitives.Longs;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import messages.*;
import org.json.JSONObject;

import java.util.Collection;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Stub {

    String endereco;
    private final Address masterAddress = Address.from("localhost:12340");
    ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();
    HashMap<String, CompletableFuture<JSONObject>> requests = new HashMap<>();
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

            System.out.println("O valor é: " + gr.value.toString());
            this.requests.get(gr.id).complete(gr.value);

        },ses);

        ms.registerHandler("putReply",(a,m) -> {
            GetReply gr = s.decode(m);

            System.out.println("O valor é: " + gr.value.toString());
            this.requests.get(gr.id).complete(gr.value);

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
            GetRequest gt = new GetRequest(rm.id, rm.key);
            ms.sendAsync(Address.from(rm.endereco), "get", s.encode(gt));

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

                TreeMap<Long,JSONObject> res = new TreeMap<>();
                for(SlaveIdentifier si : sc.slaves.values()){
                    res.putAll(sc.respostas.get(si));
                }

                sc.cf.complete(res);
            });

            for(SlaveIdentifier si : sc.slaves.values()){
                ms.sendAsync(Address.from(si.endereco), "scan", s.encode(sr.id));
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

    public CompletableFuture<JSONObject> get(long key){
        CompletableFuture<JSONObject> jsonCF = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();
        requests.put(requestID, jsonCF);

        GetRequest gt = new GetRequest(requestID, key);
        ms.sendAsync(masterAddress, "get", s.encode(gt));

        return requests.get(requestID);
    }

    public CompletableFuture<Boolean> put(long key, JSONObject value){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        PutRequest pr = new PutRequest(requestID,key,value);
        Put p = new Put(pr, cf);
        putRequests.put(requestID, p);

        ms.sendAsync(masterAddress,"put", s.encode(pr));

        return cf;

    }

    public CompletableFuture<TreeMap<Long,JSONObject>> scan(){
        CompletableFuture<TreeMap<Long,JSONObject>> cf = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();

        Scan s = new Scan(requestID,cf);
        scanRequests.put(requestID,s);

        ms.sendAsync(masterAddress,"scan", this.s.encode(requestID));

        return cf;


    }

    public static void main(String[] args) {

        String endereco = "localhost:12346";

        Stub s = new Stub(endereco);

        //s.get(10001);
        s.get(50);

        while(true){
            try {
                Thread.sleep(1000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
