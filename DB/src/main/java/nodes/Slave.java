package nodes;

import com.google.common.primitives.Longs;
import com.sun.management.OperatingSystemMXBean;
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
import java.lang.management.ManagementFactory;
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
    public Object ultimaChave;
    public LinkedHashMap<Object,JSONObject> docs;

    public ResultadoScan(Object ultimaChave, LinkedHashMap<Object, JSONObject> docs) {
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

    public TreeSet<KeysUniverse> minhasChaves = new TreeSet<>();
    // Recuperar estado
    TreeMap<KeysUniverse, Grupo> grupos = new TreeMap<>();


    HashSet<String> scanRequests = new HashSet<>();

    //Comunicacao multicast
    SpreadConnection connection = new SpreadConnection();
    private HashSet<String> replys = new HashSet<>(); //Para tratar dos pedidos repetidos dos diferentes masters
    ReentrantLock lockReplys = new ReentrantLock();

    //Distribuicao
    private HashSet<KeysUniverse> faltamJuntar = new HashSet<>();

    private boolean possoLancar = true;

    private Runnable percentagemUtilizacao = () -> {
        OperatingSystemMXBean operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        double cpu =  operatingSystemMXBean.getProcessCpuLoad();
//        double cpu =  0.75f;//operatingSystemMXBean.getProcessCpuLoad();
        float memoria = (operatingSystemMXBean.getTotalPhysicalMemorySize()-operatingSystemMXBean.getFreePhysicalMemorySize()) / (1000*1000*1000);

        TreeMap<KeysUniverse, ParEscritaLeitura> operacoes = new TreeMap<>();
        for(Map.Entry<KeysUniverse, Grupo> entry: this.grupos.entrySet()){
            Grupo g = entry.getValue();
            operacoes.put(entry.getKey(), new ParEscritaLeitura(g.escritas, g.leituras));
            g.escritas = 0;
            g.leituras = 0;
        }

        InfoMonitorizacao infoMonitorizacao = new InfoMonitorizacao(memoria, cpu, operacoes,this.id);
        SpreadMessage sm = new SpreadMessage();
        sm.setData(s.encode(infoMonitorizacao));
        sm.addGroup("master");
        sm.setAgreed(); // ao definirmos isto estamos a garantir ordem total, pelo q podemos ter varios stubs
        sm.setReliable();
        try {
            connection.multicast(sm);
        } catch (SpreadException e) {
            e.printStackTrace();
        }

        if(possoLancar){
            ses.schedule(this.percentagemUtilizacao,Config.periodoTempo,Config.unidade);
        }
        else{
            possoLancar = true;
        }
    };


    public AdvancedMessageListener aml = new AdvancedMessageListener() {
        @Override
        public void regularMessageReceived(SpreadMessage spreadMessage) {
            Object o = s.decode(spreadMessage.getData());

            if (o instanceof UpdateMessage) {
                UpdateMessage um = (UpdateMessage) o;
                KeysUniverse ku = new KeysUniverse(um.key, um.key);
                Grupo g = grupos.get(ku);


                if (g == null) {
                    System.out.println("Eu n sou do grupo como recebi update? ESTRANHO!");
                } else {
                    if (g.primario.equals(g.id)) {
//                        System.out.println("Sou o primario, n faço update!");
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

                EstadoSlave es = (EstadoSlave) o;
                Object auxKey = es.key;
                KeysUniverse ku = new KeysUniverse(auxKey, auxKey);
                Grupo g = grupos.get(ku);
                if (g == null) {
                    System.out.println("Recupera estado num grupo null!!!");
                } else {
                    if (!spreadMessage.getSender().toString().split("#")[1].equals(g.origemEstado)) {
                        System.out.println("Recebi estado de um q n é o suposto!");
                        return;
                    }

                    g.recuperaEstado(es);
                    if (es.last)
                        trataFila(g);
                }

            } else if (o instanceof PedidoEstado) {
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
            String grupo = spreadMessage.getMembershipInfo().getGroup().toString();
            HashSet<String> membros = new HashSet<>();
            try{
                for (SpreadGroup sg : spreadMessage.getMembershipInfo().getMembers()) {
                    membros.add(sg.toString().split("#")[1]);
                }
            }
            catch(Exception e){
                System.out.println("Deu excepcao");
                return ;
            }

            for (Map.Entry<KeysUniverse, Grupo> entry : grupos.entrySet()) {
                Grupo g = entry.getValue();
                if (g.grupo.equals(grupo)) {

                    g.atualiza(membros);
                    if (spreadMessage.getMembershipInfo().isCausedByJoin()) {

                        if (spreadMessage.getMembershipInfo().getJoined().toString().split("#")[1].equals(g.id)) {
                            //fui eu q me juntei
                            membros.remove(g.id);
                            if (!membros.isEmpty()) {

                                String exPrimario = Collections.min(membros); //o antigo primario
                                g.origemEstado = exPrimario;
                                String id = UUID.randomUUID().toString();
                                PedidoEstado pe = new PedidoEstado(id, entry.getKey());
                                SpreadMessage sm = new SpreadMessage();
                                sm.setData(s.encode(pe));
                                sm.addGroup(exPrimario);
                                sm.setReliable();

                                try {
                                    g.connection.multicast(sm);
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
                                sm.setReliable();

                                try {
                                    g.connection.multicast(sm);
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
                                System.out.println("Recebi um ack e n tenho o remove! ESTRANHO");
                            } else {
                                r.cf.complete(r.resposta);
                            }
                        }
                    }
                    else{

                    }
                }
            }
            else if(o instanceof LeaveGroups){
                LeaveGroups lg = (LeaveGroups)o;
                HashSet<KeysUniverse> prim = new HashSet<>();

                if(eRepetido(lg.id)){
                    return;
                }

                for(ParPrimarioSecundario pps: lg.pares){

                    Grupo g = grupos.get(pps.primario);

                    if(g != null){
                        g.leaveGroup();
                        grupos.remove(pps.primario);
                        prim.add(pps.primario);
                    }
                    else{
                        System.out.println("Grupo primario é null ao sair do grupo: " + pps.primario);
                    }

                    for(KeysUniverse ku : pps.secundarios){
                        Grupo gA = grupos.get(ku);

                        if(gA != null){
                            gA.leaveGroup();
                            grupos.remove(ku);
                        }
                        else{
                            System.out.println("Grupo secundario é null ao sair do grupo: " + ku);
                        }
                    }
                }

                LeaveGroupsReply lgr = new LeaveGroupsReply(prim,id);

                SpreadMessage sm = new SpreadMessage();
                sm.setData(s.encode(lgr));
                sm.addGroup("master");
                sm.setAgreed();
                sm.setReliable();
                try {
                    connection.multicast(sm);

                } catch (SpreadException e) {
                    e.printStackTrace();
                }

            }
            else if(o instanceof JoinGroup){
                possoLancar = false;
                JoinGroup jg =  (JoinGroup) o;

                if(eRepetido(jg.id)){
                    return;
                }

                jg.chaves.retainAll(faltamJuntar);

                for(KeysUniverse ku : jg.chaves){

                    Grupo g = grupos.get(ku);

                    if(g == null){
                        System.out.println("Grupo é null ao juntar!!!");
                    }
                    else{
                        try {
                            g.connecta(aml);
                        } catch (SpreadException e) {
                            e.printStackTrace();
                        }
                    }
                }

                faltamJuntar.removeAll(jg.chaves);
            }
            else{
                System.out.println("Recebi uma mensagem para o grupo privado que não é de ACK!");
            }
        }
    };



    public Slave(String id) {
        RocksDB.loadLibrary();

        this.id = id;

        try {
            connection.connect(InetAddress.getByName(Config.hostSpread), 0, id, false, false);
            connection.add(bml);
            SpreadGroup g = new SpreadGroup();
            g.join(connection,"global");
            SpreadGroup gr = new SpreadGroup();
            gr.join(connection,id);
        } catch (SpreadException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        boolean started = false;
        int porta = Config.portaInicial;
        this.endereco = Config.hostAtomix + ":" + porta;
        while(!started){
            try{
                ms = NettyMessagingService.builder().withAddress(Address.from(this.endereco)).build();
                ms.start().get();
                started = true;
            }
            catch(Exception e){
                System.out.println("Porta em uso: " + porta);
                porta++;
                this.endereco = Config.hostAtomix + ":" + porta;
            }
        }

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

        ses.schedule(this.percentagemUtilizacao,Config.periodoTempo,Config.unidade);
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
//                    System.out.println("Pedido de put já se encontra tratado!");
                    p.cf.thenAccept(a -> {
                        PutReply pl = new PutReply(pr.id, a);
                        ms.sendAsync(Address.from(ef.origem), "putReply", s.encode(pl));
                    });
                }

            }
            else if(ef.o instanceof RemoveRequest) {
                RemoveRequest rr = (RemoveRequest) ef.o;
                KeysUniverse ku = new KeysUniverse(rr.key, rr.key);
                Grupo grupo = grupos.get(ku);

                if (grupo == null) {
                    System.out.println("DAR MENSAGEM DE ERRO PORQUE NAO TRATAMOS DA CHAVE!!");
                } else {

                    Remove r = (Remove) grupo.requests.get(rr.id);

                    if (r == null) {
                        r = new Remove(rr, new CompletableFuture<Boolean>());
                        grupo.requests.put(rr.id, r);

                        r.cf.thenAccept(a -> {
                            RemoveReply rrp = new RemoveReply(rr.id, a);
                            ms.sendAsync(Address.from(ef.origem), "removeReply", s.encode(rrp));
                        });

                        RespostaRemove respostaRemove = grupo.remove(rr);
                        boolean resultado = respostaRemove.resposta;
                        r.setResposta(resultado);

//                        System.out.println(grupo.secundarios);
                        grupo.acks.put(rr.id, (HashSet<String>) grupo.secundarios.clone());
                        SpreadMessage sm = new SpreadMessage();
                        if (grupo.secundarios.isEmpty()) {
                            grupo.acks.remove(rr.id);
                            r.cf.complete(resultado);
                        } else {
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
                    } else {
//                        System.out.println("Pedido de remove já se encontra tratado!");

                        r.cf.thenAccept(a -> {
                            RemoveReply rrp = new RemoveReply(rr.id, a);
                            ms.sendAsync(Address.from(ef.origem), "removeReply", s.encode(rrp));
                        });
                    }
                }
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

                if (um.pr != null) {
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
//                        System.out.println("Já tinha put, pq recebi novamente???");
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
                    } else {
                        //já aconteceu algo, ver pq recebeu novo pedido
//                        System.out.println("Já tinha put, pq recebi novamente???");
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

//                    System.out.println(grupo.secundarios);
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
//                    System.out.println("Pedido de put já se encontra tratado!");
                    p.cf.thenAccept(a -> {
                        PutReply pl = new PutReply(pr.id, a);
                        ms.sendAsync(o, "putReply", s.encode(pl));
                    });
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

                    grupo.acks.put(rr.id, (HashSet<String>) grupo.secundarios.clone());
                    SpreadMessage sm = new SpreadMessage();
                    if(grupo.secundarios.isEmpty()){
                        grupo.acks.remove(rr.id);
                        r.cf.complete(resultado);
                    }
                    else {
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
//                    System.out.println("Pedido de remove já se encontra tratado!");

                    r.cf.thenAccept(a -> {
                        RemoveReply rrp = new RemoveReply(rr.id, a);
                        ms.sendAsync(o, "removeReply", s.encode(rrp));
                    });
                }







            }

        },ses);

        ms.registerHandler("scan", (o,m) -> {
            ScanRequest sr = s.decode(m);

            Grupo g = grupos.get(sr.ku);

            if(g == null){
                System.out.println("DAR MENSAGEM DE ERRO PORQUE NAO TRATAMOS DA CHAVE!!");
            }else {

                if(!g.estadoRecuperado){
                    g.fila.add(new ElementoFila(sr,o.toString()));
                    return;
                }

                scanRequests.add(sr.id); //ver depois o que acontece se já existe
                // e ver se n é melhor colocar o scan todo!!!
                ResultadoScan docs = null; //n será muito eficiente, provavelmente por causa de andar sempre a mudar o map

                //de alguma forma faz o scan à bd, ver a melhor forma

                docs = g.scan(sr);
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
                    Boolean podeEntrar = rr.podeEntrar.get(entry.getKey());
                    boolean podeE;
                    if(podeEntrar == null){
                        podeE = false;
                    }
                    else{
                        podeE = podeEntrar;
                    }
                    adicionaConexao(entry.getValue(), entry.getKey(),podeE);
                    if(!podeE){
                        this.faltamJuntar.add(entry.getKey());
                    }
                }

                /*for (Map.Entry<KeysUniverse, Grupo> as : grupos.entrySet()) {
                    System.out.println(as);
                }*/
            }
        }, ses);
    }

    public void adicionaConexao(String id, KeysUniverse grupo, boolean podeEntrar) {

        try {
            Grupo c = new Grupo(id, grupo.getGrupo(), grupo, "./localdb/" + this.id + "/", aml);
            //c.addAML(aml);
            grupos.put(grupo,c);
            if(podeEntrar){
                c.connecta(aml);
            }

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
    /*private LinkedHashMap<Long,JSONObject> getScan(Predicate<JSONObject> filtros) {

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
    }*/

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
        Slave s = new Slave(args[0]);

        while(true){
            try {
                Thread.sleep(1000000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
