package nodes;

import io.atomix.cluster.messaging.*;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import messages.*;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class  Master {

    public TreeMap<KeysUniverse,SlaveIdentifier> slaves = new TreeMap<>();
    public String endereco;
    ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();

    public Master(TreeMap<KeysUniverse, SlaveIdentifier> slaves, String endereco) {
        this.slaves = slaves;
        this.endereco = endereco;
        ms = NettyMessagingService.builder().withAddress(Address.from(endereco)).build();

        ms.start();

        this.registaHandlers();
    }

    private void registaHandlers(){

        ms.registerHandler("put",(a,m) -> {
            PutRequest pr = s.decode(m);

            SlaveIdentifier slave = slaves.get(new KeysUniverse(pr.key,pr.key));
            ReplyMaster rm = new ReplyMaster(pr.id, slave.endereco,slave.keys, pr.key);

            ms.sendAsync(a,"putMaster",s.encode(rm));

        },ses);

        ms.registerHandler("get",(a,m) -> {
            GetRequest gr = s.decode(m);
            KeysUniverse ku = new KeysUniverse(gr.key, gr.key);
            SlaveIdentifier slaveI = slaves.get(ku);
            ReplyMaster rm = new ReplyMaster(gr.id, slaveI.endereco, slaveI.keys, gr.key);

            ms.sendAsync(a,"getMaster", s.encode(rm));

        },ses);

        ms.registerHandler("remove",(a,m) -> {
            RemoveRequest rr = s.decode(m);
            KeysUniverse ku = new KeysUniverse(rr.key, rr.key);
            SlaveIdentifier slaveI = slaves.get(ku);
            ReplyMaster rm = new ReplyMaster(rr.id, slaveI.endereco, slaveI.keys, rr.key);

            ms.sendAsync(a,"removeMaster", s.encode(rm));

        },ses);

        ms.registerHandler("scan", (o,m) -> {

            ScanReply sr = new ScanReply(s.decode(m), this.slaves);

            ms.sendAsync(o, "scanMaster", s.encode(sr));
        },ses);
    }

    public void teste(){
        long key = 50;
        System.out.println("Olha o menino: " + key);
        System.out.println("Vamos ver como está o slave: " + slaves.toString());
        KeysUniverse ku = new KeysUniverse(key, key);
        System.out.println("As ku são: " + ku.toString());
        System.out.println("O hashcode é: " + ku.hashCode());
        System.out.println("Vou so fazer um equals: " + ku.equals((new KeysUniverse(0,100))));
        SlaveIdentifier slaveI = slaves.get((new KeysUniverse(0,100)));
        System.out.println("Passei: " + slaveI);
    }

    public static void main(String[] args){

        // ******* Povoamento **********
        //Para já está povoado hardecoded ...

        TreeMap<KeysUniverse,SlaveIdentifier> slaves = new TreeMap<>();
        String endereco = "localhost:12340";

        KeysUniverse ku1 = new KeysUniverse(0, 100);
        KeysUniverse ku2 = new KeysUniverse(100, 200);
        KeysUniverse ku3 = new KeysUniverse(200, 300);

        SlaveIdentifier slave1 = new SlaveIdentifier("localhost:12341", ku1);
        SlaveIdentifier slave2 = new SlaveIdentifier("localhost:12342", ku2);
        SlaveIdentifier slave3 = new SlaveIdentifier("localhost:12343", ku3);

        slaves.put(ku1, slave1);
        slaves.put(ku2, slave2);
        slaves.put(ku3, slave3);

        Master m = new Master(slaves, endereco);

        m.teste();



        while(true){
            try {
                Thread.sleep(10000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
