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
    ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();

    // Recuperar estado
    public TreeSet<KeysUniverse> minhasChaves = new TreeSet<>();
    TreeMap<KeysUniverse, Grupo> grupos = new TreeMap<>();


    HashSet<String> scanRequests = new HashSet<>();

    //Comunicacao multicast
    SpreadConnection connection = new SpreadConnection();
    private HashSet<String> replys = new HashSet<>(); //Para tratar dos pedidos repetidos dos diferentes masters
    ReentrantLock lockReplys = new ReentrantLock();


    public AdvancedMessageListener aml = new AdvancedMessageListener() {
        @Override
        public void regularMessageReceived(SpreadMessage spreadMessage) {
            System.out.println("\t\t\t\tRecebi mensagem");
            Object o = s.decode(spreadMessage.getData());
            System.out.println("\t\t\t\tRecebi mensagem: " + o.getClass());

            if (o instanceof UpdateMessage) {
                UpdateMessage um = (UpdateMessage) o;
                KeysUniverse ku = new KeysUniverse(um.key, um.key);
                Grupo g = grupos.get(ku);


                if (g == null) {
                    System.out.println("Eu n sou do grupo como recebi update? ESTRANHO!");
                } else {
                    if (g.primario.equals(g.id)) {
                        System.out.println("Sou o primario, n faço update!");
                    } else {
                        if (!g.estadoRecuperado) {
                            g.fila.add(new ElementoFila(o, spreadMessage.getSender().toString()));
                            return;
                        }

                        if (um.value != null) {
                            Put p = (Put) g.requests.get(um.id);

                            if (p == null) {
                                p = new Put(um.pr, new CompletableFuture<Boolean>(), um.resposta);
                                g.requests.put(um.id, p);

                                p.cf.thenAccept(a -> {
                                    SpreadMessage sm = new SpreadMessage();
                                    sm.addGroup(spreadMessage.getSender());
                                    sm.setData(s.encode(new ACKMessage(um.id, true, um.key)));
                                    sm.setReliable();

                                    try {
                                        g.connection.multicast(sm);
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
                            Remove r = (Remove) g.requests.get(um.id);

                            if (r == null) {
                                r = new Remove(um.rr, new CompletableFuture<Boolean>(), um.resposta);
                                g.requests.put(um.id, r);

                                r.cf.thenAccept(a -> {
                                    SpreadMessage sm = new SpreadMessage();
                                    sm.addGroup(spreadMessage.getSender());
                                    sm.setData(s.encode(new ACKMessage(um.id, false, um.key)));
                                    sm.setReliable();

                                    try {
                                        g.connection.multicast(sm);
                                    } catch (SpreadException e) {
                                        e.printStackTrace();
                                    }

                                });

                                boolean resposta = g.updateState(um);
                                r.cf.complete(resposta);
                            } else {
                                //já aconteceu algo, ver pq recebeu novo pedido
                                System.out.println("Já tinha remove, pq recebi novamente??? Porquê?");
                            }
                        }
                    }
                }
            } else if (o instanceof EstadoSlave) {
                System.out.println("Recebi mensagem com o estado!");

                EstadoSlave es = (EstadoSlave) o;
                long auxKey = es.key;
                System.out.println("Ver questao do iterator no recupera estado!");
                KeysUniverse ku = new KeysUniverse(auxKey, auxKey);
                Grupo g = grupos.get(ku);
                if (g == null) {
                    System.out.println("Recupera estado num grupo null!!!");
                } else {
                    if (!spreadMessage.getSender().toString().split("#")[1].equals(g.origemEstado)) {
                        System.out.println("Recebi estado de um q n é o suposto!");
                        return;
                    }

                    System.out.println("Vou recuperar o estado para o " + ku);
                    System.out.println("\t\t\t\t\t\tÉ o ultimo? " + es.last + "\n\n\n\n");
                    g.recuperaEstado(es);
                    System.out.println("\n\n\n" + g + "\n\n\n");
                    if (es.last)
                        trataFila(g);
                }

            } else if (o instanceof PedidoEstado) {
                System.out.println("Recebi pedido de estado");
                PedidoEstado pe = (PedidoEstado) o;

                Grupo g = grupos.get(pe.ku);

                if (g == null) {
                    System.out.println("Pedido de estado e grupo é null!!");
                } else {
                    if (!g.estadoRecuperado) {
                        System.out.println("Pedido de estado sem estado recuperado, estranho!");
                        g.fila.add(new ElementoFila(o, spreadMessage.getSender().toString()));
                    } else {

                        g.pedidoEstado(pe, spreadMessage.getSender(), s);
                    }
                }
            } else {
                System.out.println("Recebi uma mensagem que não é de update! " + o.getClass());
            }

        }


        @Override
        public void membershipMessageReceived(SpreadMessage spreadMessage) {
            System.out.println("MEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEMBERSHIP DFP");
            String grupo = spreadMessage.getMembershipInfo().getGroup().toString();
            System.out.println("\t\t\t\tRecebi uma membership message: " + grupo);
            HashSet<String> membros = new HashSet<>();
            for (SpreadGroup sg : spreadMessage.getMembershipInfo().getMembers()) {
                membros.add(sg.toString().split("#")[1]);
            }
            System.out.println(membros);

            for (Map.Entry<KeysUniverse, Grupo> entry : grupos.entrySet()) {
                Grupo g = entry.getValue();
                if (g.grupo.equals(grupo)) {

                    g.atualiza(membros);
                    if (spreadMessage.getMembershipInfo().isCausedByJoin()) {

                        if (spreadMessage.getMembershipInfo().getJoined().toString().split("#")[1].equals(g.id)) {
                            System.out.println("Juntei-me!");
                            //fui eu q me juntei
                            membros.remove(g.id);
                            if (!membros.isEmpty()) {
                                System.out.println("Enviar pedido de estado");

                                String exPrimario = Collections.min(membros); //o antigo primario
                                g.origemEstado = exPrimario;
                                String id = UUID.randomUUID().toString();
                                PedidoEstado pe = new PedidoEstado(id, entry.getKey());
                                SpreadMessage sm = new SpreadMessage();
                                sm.setData(s.encode(pe));
                                sm.addGroup(exPrimario);
                                System.out.println("VOu mandar um pedido de estado para o: " + exPrimario);
                                sm.setReliable();

                                try {
                                    g.connection.multicast(sm);
                                    System.out.println("Enviei pedido de estado");
                                } catch (SpreadException e) {
                                    e.printStackTrace();
                                }

                            } else {
                                //so existo eu, estado recuperado
                                g.estadoRecuperado = true;
                                trataFila(g);
                            }
                        }
                    } else if (spreadMessage.getMembershipInfo().isCausedByDisconnect() || spreadMessage.getMembershipInfo().isCausedByLeave()) {
                        if (spreadMessage.getMembershipInfo().getLeft().toString().split("#")[1].equals(g.origemEstado)) {
                            System.out.println("Quem me devia enviar estado falhou, pedir a outro!");

                            if(g.estadoRecuperado){
                                System.out.println("Estado já se encontra recuperado!");
                                return;
                            }
                            System.out.println("Enviar pedido de estado");
                            membros.remove(g.id);
                            if (!membros.isEmpty()) {

                                String exPrimario = Collections.min(membros); //o antigo primario
                                g.origemEstado = exPrimario;
                                String id = UUID.randomUUID().toString();
                                PedidoEstado pe;
                                if (g.estadoAMeio) {
                                    pe = new PedidoEstado(id, entry.getKey(), g.lastKey);
                                } else {
                                    pe = new PedidoEstado(id, entry.getKey());
                                }
                                SpreadMessage sm = new SpreadMessage();
                                sm.setData(s.encode(pe));
                                sm.addGroup(exPrimario);
                                System.out.println("VOu mandar um pedido de estado para o: " + exPrimario);
                                sm.setReliable();

                                try {
                                    g.connection.multicast(sm);
                                    System.out.println("Enviei pedido de estado");
                                } catch (SpreadException e) {
                                    e.printStackTrace();
                                }

                            } else {
                                //so existo eu, estado recuperado
                                g.estadoRecuperado = true;
                                trataFila(g);
                            }

                        }
                        break;
                    }
                }

            }
        }

        ;
    };

    public BasicMessageListener bml = new BasicMessageListener() {
        @Override
        public void messageReceived(SpreadMessage spreadMessage) {
            Object o = s.decode(spreadMessage.getData());

            if(o instanceof ACKMessage) {
                ACKMessage ackMessage = (ACKMessage) o;

                KeysUniverse ku = new KeysUniverse(ackMessage.key, ackMessage.key);
                Grupo grupo = grupos.get(ku);
                if (grupo == null) {
                    System.out.println("Recebi um ACK e n estou no grupo!!!");
                }
                else {


                    HashSet<String> aux = grupo.acks.get(ackMessage.id);

                    if(aux == null){
                        System.out.println("RECEBI um ack que nao devia");
                        return;
                    }

                    System.out.println("ACK, SENDER: " + spreadMessage.getSender().toString().split("#")[1]);

                    boolean rmv = aux.remove(spreadMessage.getSender().toString().split("#")[1]);

                    if (rmv && aux.isEmpty()) {

                        grupo.acks.remove(ackMessage.id);

                        if (ackMessage.put) {
                            Put p = (Put) grupo.requests.get(ackMessage.id);

                            if (p == null) {
                                System.out.println("Recebi um ack e n tenho o put! ESTRANHO");
                            } else {
                                p.cf.complete(p.resposta);
                            }
                        }
                        else {
                            Remove r = (Remove) grupo.requests.get(ackMessage.id);

                            if (r == null) {
                                System.out.println("Recebi um ack e n tenho o put! ESTRANHO");
                            } else {
                                System.out.println("Lento y contento, cara al viento -> REMOVI !!!");
                                r.cf.complete(r.resposta);
                            }
                        }
                    }
                    else{

                        System.out.println("REMOVED: " + aux);
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
            connection.add(bml);
            SpreadGroup g = new SpreadGroup();
            g.join(connection,"global");
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
        System.out.println("Encode");
        sm.setData(s.encode(rr));
        System.out.println("FEITO");
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

    private void trataFila(Grupo g) {
        if(g == null){
            System.out.println("Grupo null ao recuperar fila!");
            return;
        }

        for(ElementoFila o : g.fila){
            //tratar object como se fosse uma mensagem
            trataMensagem(o,g);
        }

        g.fila.clear();
    }

    private void trataMensagem(ElementoFila ef, Grupo g) {

        if(g.primario.equals(g.id)){
            //sou o primario
            if (ef.o instanceof PutRequest) {

                System.out.println("Recebi put");
                PutRequest pr = (PutRequest)ef.o;

                Put p = (Put) g.requests.get(pr.id);

                if (p == null) {
                        p = new Put(pr, new CompletableFuture<Boolean>());
                        g.requests.put(pr.id, p);

                    p.cf.thenAccept(a -> {
                        PutReply pl = new PutReply(pr.id, a);
                        ms.sendAsync(Address.from(ef.origem), "putReply", s.encode(pl));
                    });


                    boolean resultado = g.put(pr);
                    p.setResposta(resultado);

                    g.acks.put(pr.id, (HashSet<String>) g.secundarios.clone());
                        SpreadMessage sm = new SpreadMessage();
                        UpdateMessage um = new UpdateMessage(pr.key, pr.value, pr.id, resultado, pr);
                        sm.setData(s.encode(um));
                        sm.addGroup(g.grupo);
                    sm.setReliable();
                    try {
                                connection.multicast(sm);
                            } catch (SpreadException e) {
                        e.printStackTrace();
                    }
                }
                else{
                    System.out.println("Pedido de put já se encontra tratado!");
                }

            }
            else if(ef.o instanceof RemoveRequest) {
                System.out.println("Recuperar fila, temos de tratar o REMOVE!!!");
            }
            else if(ef.o instanceof GetRequest){
                trataGet(ef,g);
            }
            else if(ef.o instanceof ScanRequest){
                trataScan(ef,g);
            }
            else if(ef.o instanceof PedidoEstado){
                PedidoEstado pe = (PedidoEstado)ef.o;
                g.pedidoEstado(pe,ef.origem,s);
            }
            else{
                System.out.println("Sou primario e recebi: " + ef.o.getClass());
            }
        }
        else{
            //sou secundario
            if(ef.o instanceof UpdateMessage) {
                UpdateMessage um = (UpdateMessage) ef.o;

                if (um.value != null) {
                    Put p = (Put) g.requests.get(um.id);

                    if (p == null) {
                        p = new Put(um.pr, new CompletableFuture<Boolean>(), um.resposta);
                        g.requests.put(um.id, p);

                        p.cf.thenAccept(a -> {
                            SpreadMessage sm = new SpreadMessage();
                            sm.addGroup(ef.origem);
                            sm.setData(s.encode(new ACKMessage(um.id, true,um.key)));
                            sm.setReliable();

                            try {
                                g.connection.multicast(sm);
                            } catch (SpreadException e) {
                                e.printStackTrace();
                            }

                        });

                        boolean resposta = g.updateState(um);
                        p.cf.complete(resposta);
                    }
                    else {
                        //já aconteceu algo, ver pq recebeu novo pedido
                        System.out.println("Já tinha put, pq recebi novamente???");
                    }
                }
                else {
                    Remove r = (Remove) g.requests.get(um.id);

                    if (r == null) {
                        r = new Remove(um.rr, new CompletableFuture<Boolean>(), um.resposta);
                        g.requests.put(um.id, r);

                        r.cf.thenAccept(a -> {
                            SpreadMessage sm = new SpreadMessage();
                            sm.addGroup(ef.origem);
                            sm.setData(s.encode(new ACKMessage(um.id, false, um.key)));
                            sm.setReliable();

                            try {
                                g.connection.multicast(sm);
                            } catch (SpreadException e) {
                                e.printStackTrace();
                            }

                        });

                        boolean resposta = g.updateState(um);
                        r.cf.complete(resposta);
                    }
                    else {
                        //já aconteceu algo, ver pq recebeu novo pedido
                        System.out.println("Já tinha put, pq recebi novamente???");
                    }
                }
            }
            else if(ef.o instanceof GetRequest){
                trataGet(ef,g);
            }
            else if(ef.o instanceof ScanRequest){
                trataScan(ef,g);
            }
            else{
                System.out.println("Sou secundario e recebi: " + ef.o.getClass());
            }
        }
    }

    private void trataGet(ElementoFila ef, Grupo g){
        GetRequest gr = (GetRequest)ef.o;
        JSONObject resultado = g.get(gr);
        GetReply grp = new GetReply(gr.id, gr.key, resultado);
        ms.sendAsync(Address.from(ef.origem), "getReply", s.encode(grp));
    }

    private void trataScan(ElementoFila ef, Grupo g){
        ResultadoScan docs = null; //n será muito eficiente, provavelmente por causa de andar sempre a mudar o map
        ScanRequest sr = (ScanRequest) ef.o;
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
        ms.sendAsync(Address.from(ef.origem), "scanReply", s.encode(ssr));
    }

    private void criaPasta(){
        File directory = new File("localdb/"+this.id+"/");
        if (! directory.exists()){
            directory.mkdirs();
        }
    }

    private void registaHandlers(){

        //handler para responder ao pedido put, efetuado pelo stub
        ms.registerHandler("put",(o,m) -> {
            System.out.println("Recebi put");
            PutRequest pr = s.decode(m);
            //convém guardar os pedidos certo???

            KeysUniverse ku = new KeysUniverse(pr.key, pr.key);
            Grupo grupo = grupos.get(ku);
            if (grupo == null) {
                System.out.println("DAR MENSAGEM DE ERRO PORQUE NAO TRATAMOS DA CHAVE!!");
            }
            else {
                if(!grupo.estadoRecuperado){
                    grupo.fila.add(new ElementoFila(pr,o.toString()));
                    return;
                }

                Put p = (Put) grupo.requests.get(pr.id);

                if (p == null) {
                        p = new Put(pr, new CompletableFuture<Boolean>());
                        grupo.requests.put(pr.id, p);

                    p.cf.thenAccept(a -> {
                        PutReply pl = new PutReply(pr.id, a);
                        ms.sendAsync(o, "putReply", s.encode(pl));
                    });


                    boolean resultado = grupo.put(pr);
                    p.setResposta(resultado);

                    System.out.println(grupo.secundarios);
                    grupo.acks.put(pr.id, (HashSet<String>) grupo.secundarios.clone());
                    SpreadMessage sm = new SpreadMessage();
                    if(grupo.secundarios.isEmpty()){
                        grupo.acks.remove(pr.id);
                        p.cf.complete(resultado);
                    }
                    else {
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
                    System.out.println("Pedido de put já se encontra tratado!");
                }

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
                if(!g.estadoRecuperado){
                    g.fila.add(new ElementoFila(gr,a.toString()));
                    return;
                }
                JSONObject resultado = g.get(gr);
                GetReply grp = new GetReply(gr.id, gr.key, resultado);
                ms.sendAsync(a, "getReply", s.encode(grp));

            }

        },ses);


        ms.registerHandler("remove",(o,m) -> {

            System.out.println("RECEBI UM PEDIDO DE REMOVE FALTA TRATAR DOS UPDATES DO REMOVE NA PASSIVA!!!");

            RemoveRequest rr = s.decode(m);
            KeysUniverse ku = new KeysUniverse(rr.key, rr.key);
            Grupo grupo = grupos.get(ku);

            if(grupo == null){
                System.out.println("DAR MENSAGEM DE ERRO PORQUE NAO TRATAMOS DA CHAVE!!");
            }else {

                if(!grupo.estadoRecuperado){
                    grupo.fila.add(new ElementoFila(rr,o.toString()));
                    return;
                }

                Remove r = (Remove) grupo.requests.get(rr.id);

                if (r == null) {
                    r = new Remove(rr, new CompletableFuture<Boolean>());
                    grupo.requests.put(rr.id, r);

                    r.cf.thenAccept(a -> {
                        RemoveReply rrp = new RemoveReply(rr.id, a);
                        ms.sendAsync(o, "removeReply", s.encode(rrp));
                    });

                    RespostaRemove respostaRemove = grupo.remove(rr);
                    boolean resultado = respostaRemove.resposta;
                    r.setResposta(resultado);

                    System.out.println(grupo.secundarios);
                    grupo.acks.put(rr.id, (HashSet<String>) grupo.secundarios.clone());
                    SpreadMessage sm = new SpreadMessage();
                    if(grupo.secundarios.isEmpty()){
                        grupo.acks.remove(rr.id);
                        r.cf.complete(resultado);
                    }
                    else {
                        System.out.println("AINDA TEMOS DE SABER COMO É QUE VAMOS FAZER QUANDO FOR MEIO UM UPDATE");
                        UpdateMessage um = new UpdateMessage(rr.key, respostaRemove.jsonValue, rr.id, resultado, rr);
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
                    System.out.println("Pedido de put já se encontra tratado!");
                }







            }

        },ses);

        ms.registerHandler("scan", (o,m) -> {
            System.out.println("Recebi pedido de scan vindo de: " + o);
            ScanRequest sr = s.decode(m);

            Grupo g = grupos.get(sr.ku);

            if(g == null){
                System.out.println("DAR MENSAGEM DE ERRO PORQUE NAO TRATAMOS DA CHAVE!!");
            }else {

                System.out.println("Estado recuperado: " + g.estadoRecuperado);
                if(!g.estadoRecuperado){
                    g.fila.add(new ElementoFila(sr,o.toString()));
                    return;
                }

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
            System.out.println("Recebi restart");
            RestartReply rr = s.decode(m);

            if(!eRepetido(rr.id)) {

                this.minhasChaves = new TreeSet<>(rr.keys.keySet());
                for (Map.Entry<KeysUniverse, String> entry : rr.keys.entrySet()) {
                    adicionaConexao(entry.getValue(), entry.getKey());
                }

                System.out.println("\t\tVER INFOS");
                System.out.println(this.minhasChaves);
                for (Map.Entry<KeysUniverse, Grupo> as : grupos.entrySet()) {
                    System.out.println(as);
                }
                System.out.println();
                System.out.println();
            }
        }, ses);
    }

    public void adicionaConexao(String id, KeysUniverse grupo) {

        try {
            Grupo c = new Grupo(id, grupo.getGrupo(), grupo, "./localdb/" + this.id + "/", aml);
            //c.addAML(aml);
            System.out.println("Criei grupo");
            grupos.put(grupo,c);
            c.connecta(aml);
            System.out.println("Adiconei grupo");

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
