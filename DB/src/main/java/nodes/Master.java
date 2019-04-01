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
            System.out.println("Chegou-me um get");
            GetRequest gr = s.decode(m);
            System.out.println("Olha o menino: " + gr);
            System.out.println("Vamos ver como está o slave: " + slaves.toString());
            KeysUniverse ku = new KeysUniverse(gr.key, gr.key);
            System.out.println("As ku são: " + ku.toString());
            System.out.println("O hashcode é: " + ku.hashCode());
            System.out.println("Vou so fazer um equals: " + ku.equals((new KeysUniverse(0,100))));
            SlaveIdentifier slaveI = slaves.get((new KeysUniverse(0,100)));
            System.out.println("Passei: " + slaveI);
            /*if(slaves.containsKey(ku)){
                System.out.println("Eu tenho a key!");
                slaves.get(ku);
            }*/
            System.out.println("Vou buscar o endereço ... " + slaveI.endereco);
            System.out.println(("olha a key: " + slaveI.keys.toString()));
            ReplyMaster rm = new ReplyMaster(gr.id, slaveI.endereco, slaveI.keys, gr.key);
            System.out.println("Vou mandar par ao endereco: " + slaveI.endereco);

            ms.sendAsync(a,"getMaster", s.encode(rm));

        },ses);
    }

    public static void main(String[] args){

        // ******* Povoamento **********
        //Para já está povoado hardecoded ...

        HashMap<KeysUniverse,SlaveIdentifier> slaves = new HashMap<>();
        String endereco = "localhost:12340";

        KeysUniverse ku1 = new KeysUniverse(0, 100);
        KeysUniverse ku2 = new KeysUniverse(100, 200);
        KeysUniverse ku3 = new KeysUniverse(200, 300);

        SlaveIdentifier slave1 = new SlaveIdentifier("localhost:1231", ku1);
        SlaveIdentifier slave2 = new SlaveIdentifier("localhost:1232", ku2);
        SlaveIdentifier slave3 = new SlaveIdentifier("localhost:1233", ku3);

        slaves.put(ku1, slave1);
        slaves.put(ku2, slave2);
        slaves.put(ku3, slave3);

        Master m = new Master(slaves, endereco);

        while(true){
            try {
                Thread.sleep(10000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
