package nodes;

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

class ResultadoScan{
    public long ultimaChave;
    public LinkedHashMap<Long,JSONObject> docs;

    public ResultadoScan(long ultimaChave, LinkedHashMap<Long, JSONObject> docs) {
        this.ultimaChave = ultimaChave;
        this.docs = docs;
    }
}
public class Slave {

    private final Address masterAddress = Address.from("localhost:12340");
    public String endereco;
    public TreeSet<KeysUniverse> minhasChaves = new TreeSet<>();
    ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();
    Options options;
    RocksDB db;
    HashMap<String,Put> putRequests = new HashMap<>();
    HashSet<String> scanRequests = new HashSet<>();
    HashMap<KeysUniverse,RocksDB> dbs = new HashMap<>();

    public Slave(String endereco) {
        RocksDB.loadLibrary();

        this.endereco = endereco;
        ms = NettyMessagingService.builder().withAddress(Address.from(endereco)).build();

        ms.start();


        this.registaHandlers();

        System.out.println("VOu enviar uma mensagem para o master de start");
        ms.sendAsync(masterAddress,"start",s.encode(""));

        /*switch (endereco) {

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



        }*/

    }

    private void registaHandlers(){

        //handler para responder ao pedido put, efetuado pelo stub
        ms.registerHandler("put",(o,m) -> {
            PutRequest pr = s.decode(m);

            //convém guardar os pedidos certo???
            Put p = putRequests.get(pr.id);
            p.request
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
            byte[] value = null;

            try {
                value = db.get(keys);

                if(value == null){
                    GetReply grp = new GetReply(gr.id, gr.key, null);;
                    ms.sendAsync(a, "getReply", s.encode(grp));
                }else{
                    if(gr.filtros == null && gr.projecoes == null){
                        //Vai ser um get normal
                        String ret = new String(value);
                        JSONObject json = new JSONObject(ret);

                        GetReply grp = new GetReply(gr.id, gr.key, json);
                        ms.sendAsync(a, "getReply", s.encode(grp));

                    }else{
                        if(gr.projecoes != null && gr.filtros != null){
                            //Vai ser um get com projeções e filtros
                            String ret = new String(value);
                            JSONObject json = new JSONObject(ret);

                            Predicate<JSONObject> filtros = this.filtro(gr.filtros);
                            if(filtros.test(json)){
                                //Passou no filtro
                                JSONObject jsonToReturn = this.aplicaProjecao(json, gr.projecoes);

                                GetReply grp = new GetReply(gr.id, gr.key, jsonToReturn);
                                ms.sendAsync(a, "getReply", s.encode(grp));
                            }else{
                                //não passou no filtro, vai um null
                                GetReply grp = new GetReply(gr.id, gr.key, null);;
                                ms.sendAsync(a, "getReply", s.encode(grp));
                            }
                        }else{
                            if(gr.projecoes != null){
                                String ret = new String(value);
                                JSONObject json = new JSONObject(ret);

                                JSONObject jsonToReturn = this.aplicaProjecao(json, gr.projecoes);

                                GetReply grp = new GetReply(gr.id, gr.key, jsonToReturn);
                                ms.sendAsync(a, "getReply", s.encode(grp));
                            }else{
                                //são gets apenas com filtros
                                String ret = new String(value);
                                JSONObject json = new JSONObject(ret);

                                Predicate<JSONObject> filtros = this.filtro(gr.filtros);
                                if(filtros.test(json)){
                                    //Passou no filtro
                                    GetReply grp = new GetReply(gr.id, gr.key, json);
                                    ms.sendAsync(a, "getReply", s.encode(grp));
                                }else{
                                    //não passou no filtro, vai um null
                                    GetReply grp = new GetReply(gr.id, gr.key, null);;
                                    ms.sendAsync(a, "getReply", s.encode(grp));
                                }
                            }
                        }
                    }
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

                    if (rr.filtros == null && rr.projecoes == null) {
                        //Vai ser um remove normal
                        db.delete(keys);

                        RemoveReply rrp = new RemoveReply(rr.id, true);
                        ms.sendAsync(a, "removeReply", s.encode(rrp));

                    } else {
                        if (rr.projecoes != null && rr.filtros != null) {
                            //Vai ser um get com projeções e filtros
                            String ret = new String(value);
                            JSONObject json = new JSONObject(ret);

                            Predicate<JSONObject> filtros = this.filtro(rr.filtros);
                            if (filtros.test(json)) {
                                //Passou no filtro
                                for(String s: rr.projecoes)
                                    json.remove(s);

                                db.put(value, json.toString().getBytes());

                                RemoveReply rrp = new RemoveReply(rr.id, true);
                                ms.sendAsync(a, "removeReply", s.encode(rrp));
                            } else {
                                //não passou no filtro, não pode eliminar
                                RemoveReply rrp = new RemoveReply(rr.id, false);
                                ms.sendAsync(a, "removeReply", s.encode(rrp));
                            }
                        } else {
                            if (rr.projecoes != null) {
                                String ret = new String(value);
                                JSONObject json = new JSONObject(ret);

                                for(String s: rr.projecoes)
                                    json.remove(s);

                                db.put(value, json.toString().getBytes());

                                RemoveReply rrp = new RemoveReply(rr.id, true);
                                ms.sendAsync(a, "removeReply", s.encode(rrp));
                            } else {
                                //são gets apenas com filtros
                                String ret = new String(value);
                                JSONObject json = new JSONObject(ret);

                                Predicate<JSONObject> filtros = this.filtro(rr.filtros);
                                if (filtros.test(json)) {
                                    //Passou no filtro
                                    db.delete(keys);

                                    RemoveReply rrp = new RemoveReply(rr.id, true);
                                    ms.sendAsync(a, "removeReply", s.encode(rrp));
                                } else {
                                    //não passou no filtro, logo não pode eliminar
                                    RemoveReply rrp = new RemoveReply(rr.id, false);
                                    ms.sendAsync(a, "removeReply", s.encode(rrp));
                                }
                            }
                        }

                        /*db.delete(keys);

                        RemoveReply rrp = new RemoveReply(rr.id, true);
                        ms.sendAsync(a, "removeReply", s.encode(rrp));*/

                    }
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
            System.out.println("Recebi pedido de scan vindo de: " + o);
            ScanRequest sr = s.decode(m);
            scanRequests.add(sr.id); //ver depois o que acontece se já existe
            // e ver se n é melhor colocar o scan todo!!!
            ResultadoScan docs = null; //n será muito eficiente, provavelmente por causa de andar sempre a mudar o map

            //de alguma forma faz o scan à bd, ver a melhor forma
            if(sr.filtros == null){
                if(sr.projecoes == null){
                    docs = getScan(sr.nrMaximo, sr.ultimaChave,sr.ku);
                }
                /*else{
                    docs = getScan(sr.projecoes);
                }
            }
            else{
                if(sr.projecoes == null){
                    docs = getScan(filtro(sr.filtros));
                }
                else{
                    docs = getScan(filtro(sr.filtros),sr.projecoes);
                }*/

            }
            SlaveScanReply ssr = new SlaveScanReply(docs.docs,sr.ku,sr.id,docs.ultimaChave);
            ms.sendAsync(o, "scanReply", s.encode(ssr));

        },ses);

        ms.registerHandler("start", (o,m) -> {
            System.out.println("Recebi uma mensagem com a chave qe eu vou utilziar");
            KeysUniverse ku = s.decode(m);
            minhasChaves.add(ku);
            System.out.println("Vamos ver as chaves: " + minhasChaves.toString());
            try {
                this.options = new Options().setCreateIfMissing(true);
                RocksDB ndb = RocksDB.open(options, "./localdb/" + endereco.replaceAll(":", "") + "-" + ku.toString() + "/" );
                dbs.put(ku,ndb);

            } catch (RocksDBException e) {
                System.out.println("Exceçãoooooooooooooooo: " + e.getMessage());
            }
        }, ses);
    }

    private JSONObject aplicaProjecao(JSONObject o, HashMap<Boolean, ArrayList<String>> p){
        ArrayList<String> f = p.get(false);
        if(f != null){
            for(String aux: f){
                o.remove(aux);
            }
        }
        ArrayList<String> t = p.get(true);
        if(f != null){
            JSONObject aux = o;
            o = new JSONObject();
            for(String a: f){
                o.put(a,aux.get(a));
            }
        }

        return o;
    }

    //scan para todos os objectos, sem projecções
    private ResultadoScan getScan(int nrMaximo, long ultimaChave, KeysUniverse ku) {
            System.out.println("----------------------------Novo pedido scan: " + ku + " ---------------------------");
            LinkedHashMap<Long,JSONObject> docs = new LinkedHashMap<>();
            RocksIterator iterador = db.newIterator();
            int quantos = 0;
            long chave = -1;
            long anterior = -1;
            if(ultimaChave == -1) {
                ultimaChave = ku.min;
                iterador.seek(Longs.toByteArray(ultimaChave));
                chave = ultimaChave;
            }
            else{
                iterador.seek(Longs.toByteArray(ultimaChave));
                if(!iterador.isValid()){
                    return null;
                }
                iterador.next();
            }



            while (iterador.isValid()) {
                long k = Longs.fromByteArray(iterador.key());
                System.out.println("Key: " + k);
                if(k <= anterior){
                    //n está neste universe
                    System.out.println("Deu a volta");
                    break;
                }
                anterior = k;
                chave = k;
                String v = new String(iterador.value());
                JSONObject json = new JSONObject(v);
                docs.put(k,json);
                quantos++;
                if(quantos >= nrMaximo){
                    System.out.println("Atingi o máximo");
                    break;
                }
                iterador.next();
            }
            System.out.println("---------------FIM-----------------------");
            return new ResultadoScan(chave,docs);
    }

    //scan para todos os objectos, com projecções
    private LinkedHashMap<Long,JSONObject> getScan(HashMap<Boolean,ArrayList<String>> p) {

        LinkedHashMap<Long,JSONObject> docs = new LinkedHashMap<>();
        RocksIterator iterador = db.newIterator();
        iterador.seekToFirst();
        while (iterador.isValid()) {
            long k = Longs.fromByteArray(iterador.key());
            String v = new String(iterador.value());
            JSONObject json = new JSONObject(v);
            docs.put(k,aplicaProjecao(json,p)); //aplica as projecções aos objectos
            iterador.next();
        }
        return docs;
    }

    //scan com filtros, sem projecções
    private LinkedHashMap<Long,JSONObject> getScan(Predicate<JSONObject> filtros) {

        LinkedHashMap<Long,JSONObject> docs = new LinkedHashMap<>();
        RocksIterator iterador = db.newIterator();
        iterador.seekToFirst();
        while (iterador.isValid()) {
            long k = Longs.fromByteArray(iterador.key());
            String v = new String(iterador.value());
            JSONObject json = new JSONObject(v);
            if(filtros.test(json)) {
                //adiciona apenas se passar nos filtros
                docs.put(k,json);
            }
            iterador.next();
        }
        return docs;
    }

    //scan com filtros, com projecções
    private LinkedHashMap<Long,JSONObject> getScan(Predicate<JSONObject> filtros, HashMap<Boolean,ArrayList<String>> p) {

        LinkedHashMap<Long,JSONObject> docs = new LinkedHashMap<>();
        RocksIterator iterador = db.newIterator();
        iterador.seekToFirst();
        while (iterador.isValid()) {
            long k = Longs.fromByteArray(iterador.key());
            String v = new String(iterador.value());
            JSONObject json = new JSONObject(v);
            if(filtros.test(json)) {
                //adiciona apenas se passar nos filtros
                docs.put(k,aplicaProjecao(json,p)); //aplica as projeções ao objecto
            }
            iterador.next();
        }
        return docs;
    }

    //função que cria o filtro a partir dos vários filtros
    private Predicate<JSONObject> filtro(ArrayList<Predicate<JSONObject>> filters) {

        Predicate<JSONObject> pred = filters.stream().reduce(Predicate::and).orElse(x -> true);
        pred = pred.negate();

        return pred;
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
