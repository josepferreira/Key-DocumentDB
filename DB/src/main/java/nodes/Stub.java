package nodes;

import com.google.common.primitives.Longs;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import messages.*;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Stub {

    String endereco;
    private final String masterAddress = "localhost:12340";
    ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();
    HashMap<String, CompletableFuture<JSONObject>> requests = new HashMap<>();

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

    }

    public CompletableFuture<JSONObject> get(long key){
        CompletableFuture<JSONObject> jsonCF = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();
        requests.put(requestID, jsonCF);

        GetRequest gt = new GetRequest(requestID, key);
        ms.sendAsync(Address.from(masterAddress), "get", s.encode(gt));

        return requests.get(requestID);
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
