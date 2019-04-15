package nodes;

import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import messages.*;
import org.json.JSONObject;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

public class Slave {

    public String endereco;
    public KeysUniverse minhasChaves;
    ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();
    Options options;
    RocksDB db;
    HashMap<String,Put> putRequests = new HashMap<>();
    HashSet<String> scanRequests = new HashSet<>();

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

        //handler para responder ao pedido put, efetuado pelo stub
        ms.registerHandler("put",(o,m) -> {
            PutRequest pr = s.decode(m);

            //convém guardar os pedidos certo???
            Put p = putRequests.get(pr.id);
            if(p == null){
                p = new Put(pr,new CompletableFuture<Boolean>());
                putRequests.put(pr.id,p);

                p.cf.thenAccept(a -> {
                    PutReply pl = new PutReply(pr.id,a);
                    ms.sendAsync(o, "putReply", s.encode(pl));
                });

                ////se ainda n inseriu insere
                byte[] key = Longs.toByteArray(pr.key);
                try {
                    db.put(key, pr.value.toString().getBytes());
                    p.cf.complete(true);
                } catch (RocksDBException e) {
                    p.cf.complete(false);
                    e.printStackTrace();
                    System.out.println("Erro ao realizar put na BD local!");
                }
            }
            else{
                //já aconteceu algo, ver pq recebeu novo pedido
            }

        },ses);


        // **** Handler para responder a um getefetuado pelo stub
        ms.registerHandler("get",(a,m) -> {

            GetRequest gr = s.decode(m);
            byte[] keys = Longs.toByteArray(gr.key);

            try {
                byte[] value = db.get(keys);

                if(value == null){
                    GetReply grp = new GetReply(gr.id, gr.key, null);;
                    ms.sendAsync(a, "getReply", s.encode(grp));
                }else {
                    String ret = new String(value);
                    JSONObject json = new JSONObject(ret);

                    GetReply grp = new GetReply(gr.id, gr.key, json);
                    ms.sendAsync(a, "getReply", s.encode(grp));

                }

            } catch (RocksDBException e) {
                e.printStackTrace();
                GetReply grp = new GetReply(gr.id, gr.key, null);
                ms.sendAsync(a, "getReply", s.encode(grp));
            } catch (Exception e) {
                System.out.println("ERRO NA STRING: " + e.getMessage());
                GetReply grp = new GetReply(gr.id, gr.key, null);
                ms.sendAsync(a, "getReply", s.encode(grp));
            }


        },ses);


        ms.registerHandler("remove",(a,m) -> {

            RemoveRequest rr = s.decode(m);
            byte[] keys = Longs.toByteArray(rr.key);

            try {
                byte[] value = db.get(keys);

                if(value == null){
                    RemoveReply rrp = new RemoveReply(rr.id, false);
                    ms.sendAsync(a, "removeReply", s.encode(rrp));
                }else {


                    db.delete(keys);

                    RemoveReply rrp = new RemoveReply(rr.id, true);
                    ms.sendAsync(a, "removeReply", s.encode(rrp));

                }

            } catch (RocksDBException e) {
                e.printStackTrace();
                System.out.println("DEU PROBLEMAS A ELIMINAR O VALOR ...");
                RemoveReply rrp = new RemoveReply(rr.id, false);
                ms.sendAsync(a, "removeReply", s.encode(rrp));
            } catch (Exception e) {
                System.out.println("ERRO NA STRING: " + e.getMessage());
                RemoveReply rrp = new RemoveReply(rr.id, false);
                ms.sendAsync(a, "removeReply", s.encode(rrp));
            }


        },ses);

        ms.registerHandler("scan", (o,m) -> {

            String id = s.decode(m);
            scanRequests.add(id); //ver depois o que acontece se já existe
            LinkedHashMap<Long,JSONObject> docs = new LinkedHashMap<>(); //n será muito eficiente, provavelmente por causa de andar sempre a mudar o map

            //de alguma forma faz o scan à bd, ver a melhor forma
            docs = getScan();

            SlaveScanReply ssr = new SlaveScanReply(docs,id);

            ms.sendAsync(o, "scanReply", s.encode(ssr));

        },ses);
    }

    private LinkedHashMap<Long,JSONObject> getScan() {

            LinkedHashMap<Long,JSONObject> docs = new LinkedHashMap<>();
            RocksIterator iterador = db.newIterator();
            iterador.seekToFirst();
            while (iterador.isValid()) {
                long k = Longs.fromByteArray(iterador.key());
                String v = new String(iterador.value());
                JSONObject json = new JSONObject(v);
                docs.put(k,json);
                iterador.next();
            }
            return docs;
    }

    //função que filtra um conjunto de documentos segundo uma lista de filtros
    private Map<Long,JSONObject> filtro(Map<Long,JSONObject> docs, ArrayList<Predicate<JSONObject>> filters) {

        Predicate<JSONObject> pred = filters.stream().reduce(Predicate::and).orElse(x -> true);
        pred = pred.negate();
        docs.values().removeIf(pred);

        return docs;
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
