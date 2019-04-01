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
            this.db = RocksDB.open(options, "./localdb/" + endereco.replaceAll(":", "") + "/");

        } catch (RocksDBException e) {
            System.out.println("Exceçãoooooooooooooooo: " + e.getMessage());
        }

        this.registaHandlers();

        switch (endereco) {

            case "localhost:12341":
                long aux = 50;
                byte[] key1 = Longs.toByteArray(aux);
                JSONObject json = new JSONObject();
                json.put("ola1", 1);
                try {
                    db.put(key1, json.toString().getBytes());
                } catch (RocksDBException e) {
                    e.printStackTrace();

                }
            case "localhost:12342":
                long aux2 = 150;
                byte[] key2 = Longs.toByteArray(aux2);
                JSONObject json2 = new JSONObject();
                json2.put("ola2", 2);
                try {
                    db.put(key2, json2.toString().getBytes());
                } catch (RocksDBException e) {
                    e.printStackTrace();

                }
            case "localhost:12343":
                long aux3 = 250;
                byte[] key3 = Longs.toByteArray(aux3);
                JSONObject json3 = new JSONObject();
                json3.put("ol3", 3);
                try {
                    db.put(key3, json3.toString().getBytes());
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }

        }

    }

    private void registaHandlers(){

        /*ms.registerHandler("put",(a,m) -> {
            PutRequest pr = s.decode(m);

            SlaveIdentifier slave = slaves.get(new KeysUniverse(pr.key,pr.key));
            ReplyMaster rm = new ReplyMaster(slave.endereco,slave.keys);

            ms.sendAsync(a,"putMaster",s.encode(rm));

        },ses);*/


        // **** Handler para responder a um getefetuado pelo stub
        ms.registerHandler("get",(a,m) -> {

            GetRequest gr = s.decode(m);
            byte[] keys = Longs.toByteArray(gr.key);

            try {

                byte[] value = db.get(keys);

                if(value == null){
                    GetReply grp = new GetReply(gr.id, gr.key, null);
                    ms.sendAsync(a, "getReply", s.encode(grp));
                }else {

                    String ret = new String(value);
                    JSONObject json = new JSONObject(ret);

                    GetReply grp = new GetReply(gr.id, gr.key, json);
                    ms.sendAsync(a, "getReply", s.encode(grp));

                }

            } catch (RocksDBException e) {
                e.printStackTrace();
            } catch (Exception e) {
                System.out.println("ERRO NA STRING: " + e.getMessage());
            }


        },ses);
    }


    @Override
    public String toString() {
        return "Slave{" +
                "endereco='" + endereco +
                '}';
    }

    public static void main(String[] args) {

        //Para já o valor do args deve de ser 1 ou 2 ou 3
        Slave s = new Slave("localhost:1234" + args[0]);

        while(true){
            try {
                Thread.sleep(1000000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
