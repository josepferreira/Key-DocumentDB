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

        ms.registerHandler("getReply",(a,m) -> {
            GetReply gr = s.decode(m);

            System.out.println("O valor é: " + gr.value.toString());
            this.requests.get(gr.id).complete(gr.value);

        },ses);

    }

    public CompletableFuture<JSONObject> get(long key){
        CompletableFuture<JSONObject> jsonCF = new CompletableFuture<>();
        String requestID = UUID.randomUUID().toString();
        requests.put(requestID, jsonCF);

        GetRequest gt = new GetRequest(requestID, key);
        ms.sendAsync(Address.from("localhost:12345"), "get", s.encode(gt));

        return requests.get(requestID);
    }

    public static void main(String[] args) {

        String endereco = "localhost:12346";

        Stub s = new Stub(endereco);

        s.get(10001);
        s.get(100);

        while(true){
            try {
                Thread.sleep(1000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
