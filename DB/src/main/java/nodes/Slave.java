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
import spread.*;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
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

    //private final Address masterAddress = Address.from("localhost:12340");
    public String endereco;
    public String id;
    public TreeSet<KeysUniverse> minhasChaves = new TreeSet<>();
    ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();

    HashMap<String,Put> putRequests = new HashMap<>();
    HashMap<String,Remove> removeRequests = new HashMap<>();
    HashSet<String> scanRequests = new HashSet<>();

    TreeMap<KeysUniverse, Grupo> grupos = new TreeMap<>();
    HashMap<String, HashSet<String>> acks = new HashMap<>();

    //Comunicacao multicast
    SpreadConnection connection = new SpreadConnection();
    private HashSet<String> replys = new HashSet<>(); //Para tratar dos pedidos repetidos dos diferentes masters
    ReentrantLock lockReplys = new ReentrantLock();


    public AdvancedMessageListener aml = new AdvancedMessageListener() {
        @Override
        public void regularMessageReceived(SpreadMessage spreadMessage) {
            Object o = s.decode(spreadMessage.getData());

            if(o instanceof UpdateMessage) {
                UpdateMessage um = (UpdateMessage) o;
                KeysUniverse ku = new KeysUniverse(um.key, um.key);
                Grupo g = grupos.get(ku);

                if (g == null) {
                    System.out.println("Eu n sou do grupo como recebi update? ESTRANHO!");
                } else {
                    if (g.primario.equals(g.id)) {
                        System.out.println("Sou o primario, n faço update!");
                    } else {

                        if (um.value != null) {
                            Put p = putRequests.get(um.id);

                            if (p == null) {
                                p = new Put(um.pr, new CompletableFuture<Boolean>(), um.resposta);
                                putRequests.put(um.id, p);

                                p.cf.thenAccept(a -> {
                                    SpreadMessage sm = new SpreadMessage();
                                    sm.addGroup(spreadMessage.getSender());
                                    sm.setData(s.encode(new ACKMessage(um.id, true)));
                                    sm.setReliable();

                                    try {
                                        connection.multicast(sm);
                                    } catch (SpreadException e) {
                                        e.printStackTrace();
                                    }

                                });

                                boolean resposta = g.updateState(um);
                                p.cf.complete(resposta);
                            } else {
                                //já aconteceu algo, ver pq recebeu novo pedido
                                System.out.println("Já tinha put, pq recebi novamente???");
                            }

                        } else {
                            System.out.println("REMOVE!!! Tratar depois! TEMOS!");
                        }
                    }
                }
            }
            else{
                System.out.println("Recebi uma mensagem que não é de update! " + o.getClass());
            }

        }

        @Override
        public void membershipMessageReceived(SpreadMessage spreadMessage) {
            String grupo = spreadMessage.getGroups()[0].toString();

            HashSet<String> membros = new HashSet<>();
            for(SpreadGroup sg : spreadMessage.getMembershipInfo().getMembers()){
                membros.add(sg.toString());
            }

            for(Grupo g: grupos.values()){
                if(g.grupo.equals(grupo)){
                    g.atualiza(membros);
                    break;
                }
            }

        }
    };

    public BasicMessageListener bml = new BasicMessageListener() {
        @Override
        public void messageReceived(SpreadMessage spreadMessage) {
            Object o = s.decode(spreadMessage.getData());

            if(o instanceof ACKMessage){
                ACKMessage ackMessage = (ACKMessage)o;

                HashSet<String> aux = acks.get(ackMessage.id);

                System.out.println("ACK, SENDER: " + spreadMessage.getSender().toString());

                boolean rmv = aux.remove(spreadMessage.getSender().toString());

                if(rmv && aux.isEmpty()){
                    if(ackMessage.put){
                        Put p = putRequests.get(ackMessage.id);

                        if(p == null){
                            System.out.println("Recebi um ack e n tenho o put! ESTRANHO");
                        }
                        else{
                            p.cf.complete(p.resposta);
                        }
                    }
                    else{
                        //e remove
                        System.out.println("Recebi ack remove, ver o q fazer!");
                    }
                }
            }
            else{
                System.out.println("Recebi uma mensagem para o grupo privado que não é de ACK!");
            }
        }
    };



    public Slave(String endereco, String id) {
        RocksDB.loadLibrary();

        this.endereco = endereco;
        this.id = id;

        try {
            connection.connect(InetAddress.getByName("localhost"), 0, id, false, false);
        } catch (SpreadException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        ms = NettyMessagingService.builder().withAddress(Address.from(endereco)).build();

        ms.start();

        criaPasta();

        this.registaHandlers();

        //Necessario criar o slave o random porque senao os masters vao criar todos um diferente ...
        System.out.println("VOu enviar uma mensagem para o master de restart");
        RestartRequest rr = new RestartRequest(this.id, this.endereco);
        SpreadMessage sm = new SpreadMessage();
        sm.setData(s.encode(rr));
        sm.addGroup("master");
        sm.setAgreed(); // ao defiirmos isto estamos a garantir ordem total, pelo q podemos ter varios stubs
        sm.setReliable();
        try {
            connection.multicast(sm);
        } catch (SpreadException e) {
            e.printStackTrace();
        }
        /*ms.sendAsync(masterAddress,"start",s.encode(sr));*/

    }

    /**
     * Serve para remover o estado das respostas, para evitar que cresça infinitamente
     * @param id
     */
    private void removeReply(String id){
        //VAMOS PRECISAR DE CONTROLO DE CONCORRENCIA MUITO SIMPLES!!!!
        try {
            lockReplys.lock();
            replys.remove(id);
        }finally {
            lockReplys.unlock();
        }
    }

    /**
     * Necessário usar este runnable desta forma para conseguir passar um parametro
     * @param id
     * @return
     */
    private Runnable delete(final String id){
        Runnable ret = new Runnable() {
            @Override
            public void run() {
                removeReply(id);
            }
        };

        return ret;
    }

    /**
     * Verifica se já possui uma resposta repetida
     * Caso não exista, adiciona ao hashset e cria o schedule
     * @param id
     * @return
     */
    private boolean eRepetido(String id){

        try {

            lockReplys.lock();

            if (!this.replys.contains(id)) {
                replys.add(id);
                this.ses.schedule(delete(id), 60, TimeUnit.SECONDS);
                return false;
            }

            return true;

        }finally {
            lockReplys.unlock();
        }
    }

    private void criaPasta(){
        File directory = new File("localdb/"+this.id+"/");
        if (! directory.exists()){
            directory.mkdirs();
            // If you require it to make the entire directory path including parents,
            // use directory.mkdirs(); here instead.
        }
    }

    private void registaHandlers(){

        //handler para responder ao pedido put, efetuado pelo stub
        ms.registerHandler("put",(o,m) -> {
            System.out.println("Recebi put");
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

                KeysUniverse ku = new KeysUniverse(pr.key, pr.key);
                Grupo grupo = grupos.get(ku);

                if(grupo == null){
                    System.out.println("DAR MENSAGEM DE ERRO PORQUE NAO TRATAMOS DA CHAVE!!");
                }else {
                    boolean resultado = grupo.put(pr);
                    p.setResposta(resultado);

                    acks.put(pr.id, (HashSet<String>) grupo.secundarios.clone());
                    SpreadMessage sm = new SpreadMessage();
                    UpdateMessage um = new UpdateMessage(pr.key, pr.value, pr.id, resultado, pr);
                    sm.setData(s.encode(um));
                    sm.addGroup(grupo.grupo);
                    sm.setReliable();
                    try {
                        connection.multicast(sm);
                    } catch (SpreadException e) {
                        e.printStackTrace();
                    }
                }

            }
            else{
                //já aconteceu algo, ver pq recebeu novo pedido
            }

        },ses);


        // **** Handler para responder a um getefetuado pelo stub
        ms.registerHandler("get",(a,m) -> {

            GetRequest gr = s.decode(m);
            KeysUniverse ku = new KeysUniverse(gr.key, gr.key);
            Grupo g = grupos.get(ku);

            if(g == null){
                System.out.println("DAR MENSAGEM DE ERRO PORQUE NAO TRATAMOS DA CHAVE!!");
            }else {

                JSONObject resultado = g.get(gr);
                GetReply grp = new GetReply(gr.id, gr.key, resultado);
                ms.sendAsync(a, "getReply", s.encode(grp));

            }

        },ses);


        ms.registerHandler("remove",(a,m) -> {

            System.out.println("RECEBI UM PEDIDO DE REMOVE FALTA TRATAR DOS UPDATES DO REMOVE NA PASSIVA!!!");

            RemoveRequest rr = s.decode(m);
            KeysUniverse ku = new KeysUniverse(rr.key, rr.key);
            Grupo g = grupos.get(ku);

            if(g == null){
                System.out.println("DAR MENSAGEM DE ERRO PORQUE NAO TRATAMOS DA CHAVE!!");
            }else {

                boolean resultado = g.remove(rr);
                RemoveReply rrp = new RemoveReply(rr.id, resultado);
                ms.sendAsync(a, "removeReply", s.encode(rrp));

            }

        },ses);

        ms.registerHandler("scan", (o,m) -> {
            System.out.println("Recebi pedido de scan vindo de: " + o);
            ScanRequest sr = s.decode(m);

            Grupo g = grupos.get(sr.ku);

            if(g == null){
                System.out.println("DAR MENSAGEM DE ERRO PORQUE NAO TRATAMOS DA CHAVE!!");
            }else {
                scanRequests.add(sr.id); //ver depois o que acontece se já existe
                // e ver se n é melhor colocar o scan todo!!!
                ResultadoScan docs = null; //n será muito eficiente, provavelmente por causa de andar sempre a mudar o map

                //de alguma forma faz o scan à bd, ver a melhor forma
                if (sr.filtros == null) {
                    if (sr.projecoes == null) {
                        docs = g.scan(sr);
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
                SlaveScanReply ssr = new SlaveScanReply(docs.docs, sr.ku, sr.id, docs.ultimaChave);
                ms.sendAsync(o, "scanReply", s.encode(ssr));
            }
        },ses);

        ms.registerHandler("restart", (o,m) -> {
            RestartReply rr = s.decode(m);

            this.minhasChaves = (TreeSet<KeysUniverse>) rr.keys.keySet();
            for(Map.Entry<KeysUniverse, String> entry: rr.keys.entrySet()){
                adicionaConexao(entry.getValue(), entry.getKey());
            }

        }, ses);

        /*ms.registerHandler("start", (o,m) -> {
            System.out.println("Recebi uma mensagem com a chave qe eu vou utilziar");
            try {
                StartReply sr = s.decode(m);
                System.out.println("Passei o decode ...");
                if (!eRepetido(sr.id)) {

                    KeysUniverse ku = sr.keys;
                    minhasChaves.add(ku);
                    System.out.println("Vamos ver as chaves: " + minhasChaves.toString());
                    try {
                        this.options = new Options().setCreateIfMissing(true);
                        RocksDB ndb = RocksDB.open(options, "./localdb/" + endereco.replaceAll(":", "") + "-" + ku.toString() + "/");
                        dbs.put(ku, ndb);

                    } catch (RocksDBException e) {
                        System.out.println("Exceçãoooooooooooooooo: " + e.getMessage());
                    }

                }
            }catch (Exception e){
                System.out.println(e.getMessage());
            }
        }, ses);

        ms.registerHandler("startFirst", (o,m) -> {
            System.out.println("Recebi uma mensagem de start com id!");
            SpreadConnection myconnection = new SpreadConnection();
            Object obj = s.decode(m);
            StartRequest sr = (StartRequest)obj;
            try {
                myconnection.connect(InetAddress.getByName("localhost"), 0, sr.id, false, false);
            } catch (SpreadException e) {
                e.printStackTrace();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }, ses);*/
    }

    public void adicionaConexao(String id, KeysUniverse grupo) {

        try {
            System.out.println("ATENCAO QUE O GRUPO ESTA NULO!!!");
            Grupo c = new Grupo(id, grupo.getGrupo(), grupo, "./localdb/" + id + "/");
            grupos.put(grupo,c);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (SpreadException e) {
            e.printStackTrace();
        }
    }


    public void desconecta(KeysUniverse grupo) throws SpreadException{
        Grupo c = grupos.remove(grupo);
        if(c != null){
            c.desconectar();
        }
    }


    //scan para todos os objectos, com projecções
   /* private LinkedHashMap<Long,JSONObject> getScan(HashMap<Boolean,ArrayList<String>> p) {

        LinkedHashMap<Long,JSONObject> docs = new LinkedHashMap<>();
        RocksDB db = null;
        System.out.println("\t\t\t\t\tPRECISAAAAAAAAAAAAAAAAAAAAAAAAAMOS VER MUDAR ISTO!!! A BASE DE DADOS ESTA A NULL DE PREPOSITO!!!");
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
*/
    //scan com filtros, sem projecções
    private LinkedHashMap<Long,JSONObject> getScan(Predicate<JSONObject> filtros) {

        LinkedHashMap<Long,JSONObject> docs = new LinkedHashMap<>();
        System.out.println("\t\t\t\t\tPRECISAAAAAAAAAAAAAAAAAAAAAAAAAMOS VER MUDAR ISTO!!! A BASE DE DADOS ESTA A NULL DE PREPOSITO!!!");
        RocksDB db = null;
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
    /*private LinkedHashMap<Long,JSONObject> getScan(Predicate<JSONObject> filtros, HashMap<Boolean,ArrayList<String>> p) {

        LinkedHashMap<Long,JSONObject> docs = new LinkedHashMap<>();
        RocksDB db = null;
        RocksIterator iterador = db.newIterator();
        System.out.println("\t\t\t\t\tPRECISAAAAAAAAAAAAAAAAAAAAAAAAAMOS VER MUDAR ISTO!!! A BASE DE DADOS ESTA A NULL DE PREPOSITO!!!");
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
    }*/



    @Override
    public String toString() {
        return "Slave{" +
                "endereco='" + endereco +
                '}';
    }

    public static void main(String[] args) {

        //Para já o valor do args deve de ser 1 ou 2 ou 3
        Slave s = new Slave("localhost:1234" + args[0],args[1]);

        while(true){
            try {
                Thread.sleep(1000000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
