package nodes;

import com.google.common.primitives.Longs;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import messages.GetReply;
import messages.GetRequest;
import messages.PutRequest;
import messages.ReplyMaster;
import org.json.JSONObject;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Slave {

    public String endereco;
    ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();
    Options options;
    RocksDB db;

    public Slave(String endereco) {
        RocksDB.loadLibrary();

        this.endereco = endereco;
        ms = NettyMessagingService.builder().withAddress(Address.from(endereco)).build();

        ms.start();

        try {
            this.options = new Options().setCreateIfMissing(true);
            this.db = RocksDB.open(options, "./teste");

        } catch (RocksDBException e) {
            System.out.println("Exceçãoooooooooooooooo: " + e.getMessage());
        }

        this.registaHandlers();

        long aux = 100;
        byte[] key1 = Longs.toByteArray(aux);

        JSONObject json = new JSONObject();
        json.put("ola", 1);

        try {
            db.put(key1, json.toString().getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
            System.out.println("No inserir");
        }

    }

    private void registaHandlers(){

        /*ms.registerHandler("put",(a,m) -> {
            PutRequest pr = s.decode(m);

            SlaveIdentifier slave = slaves.get(new KeysUniverse(pr.key,pr.key));
            ReplyMaster rm = new ReplyMaster(slave.endereco,slave.keys);

            ms.sendAsync(a,"putMaster",s.encode(rm));

        },ses);*/

        ms.registerHandler("get",(a,m) -> {
            GetRequest gr = s.decode(m);
            byte[] keys = Longs.toByteArray(gr.key);
            System.out.println("Tiu no get");
            try {
                System.out.println("Ola");
                byte[] value = db.get(keys);
                System.out.println("ole");
                String ret = new String(value);
                JSONObject json = new JSONObject(ret);

                GetReply grp = new GetReply(gr.id, gr.key, json);
                ms.sendAsync(a, "getReply", s.encode(grp));
            } catch (RocksDBException e) {
                e.printStackTrace();
                System.out.println("la la la");
            } catch (Exception e) {
                System.out.println("ERRO NA STRING!!");
            }


        },ses);
    }

    public static void main(String[] args) {

        Slave s = new Slave("localhost:12345");

        while(true){
            try {
                Thread.sleep(1000000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
