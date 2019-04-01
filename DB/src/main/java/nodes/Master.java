package nodes;

import io.atomix.cluster.messaging.*;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import messages.*;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Master {

    public HashMap<KeysUniverse,SlaveIdentifier> slaves = new HashMap<>();
    public String endereco;
    ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();

    public Master(HashMap<KeysUniverse, SlaveIdentifier> slaves, String endereco) {
        this.slaves = slaves;
        this.endereco = endereco;
        ms = NettyMessagingService.builder().withAddress(Address.from(endereco)).build();
    }

    private void registaHandlers(){

        ms.registerHandler("put",(a,m) -> {
            PutRequest pr = s.decode(m);

            SlaveIdentifier slave = slaves.get(new KeysUniverse(pr.key,pr.key));
            ReplyMaster rm = new ReplyMaster(pr.id, slave.endereco,slave.keys);

            ms.sendAsync(a,"putMaster",s.encode(rm));

        },ses);

        ms.registerHandler("get",(a,m) -> {
            PutRequest pr = s.decode(m);

            SlaveIdentifier slave = slaves.get(new KeysUniverse(pr.key,pr.key));
            ReplyMaster rm = new ReplyMaster(pr.id, slave.endereco,slave.keys);

            ms.sendAsync(a,"getMaster",s.encode(rm));

        },ses);
    }
}
