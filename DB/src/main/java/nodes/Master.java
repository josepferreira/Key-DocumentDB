package nodes;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import io.atomix.cluster.messaging.*;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import messages.*;
import spread.*;

import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class  Master {

    public final String idSlave = "slave";

    public String endereco;
    ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();
    SpreadConnection connection = new SpreadConnection();
    SpreadConnection connectionGlobal = new SpreadConnection();

    //estado partilhado
    public final int fatorReplicacao = 1;
    public int nSlaves = 0;
    public TreeMap<KeysUniverse,SlaveIdentifier> slaves = new TreeMap<>();
    private HashSet<String> start = new HashSet<>();
    private HashMap<String, TreeSet<KeysUniverse>> keysSlaves = new HashMap<>();

    private ArrayList<Object> fila = new ArrayList<>();
    private HashSet<String> pedidosEstado = new HashSet<>();
    private boolean estadoRecuperado;
    private boolean descarta;



    BasicMessageListener bml = new BasicMessageListener() {
        @Override
        public void messageReceived(SpreadMessage spreadMessage) {
            byte[] msg = spreadMessage.getData();
            Object o = s.decode(msg);
            System.out.println("GROUP:" + spreadMessage.getSender());

            if(descarta){
                if(o instanceof PedidoEstadoMaster){
                    System.out.println("Recebi pedido master");
                    descarta = false;
                }
            }
            else{
                if(!estadoRecuperado){
                    if(o instanceof EstadoMaster){
                        System.out.println("Recebi resposta pedido estado");
                        //recupera estado
                        EstadoMaster em = (EstadoMaster)o;
                        if(!pedidosEstado.contains(em.id)){
                            System.out.println("Vou alterar estado");
                            pedidosEstado.add(em.id);
                            slaves.putAll(em.slaves);
                            start.addAll(em.start);
                            estadoRecuperado = true;
                            trataFila();
                        }
                    }
                    else{
                        fila.add(o);
                    }
                }
                else{
                    if(o instanceof PedidoEstadoMaster){
                        System.out.println("Vou responder ao pedido de estado");
                        EstadoMaster em = new EstadoMaster(((PedidoEstadoMaster)o).id,slaves,start);
                        SpreadMessage sm = new SpreadMessage();
                        sm.setData(s.encode(em));
                        sm.addGroup(spreadMessage.getSender());
                        sm.setReliable();
                        try {
                            connection.multicast(sm);
                        } catch (SpreadException e) {
                            e.printStackTrace();
                        }
                    }
                    else
                        trataMensagem(o);
                }
            }
        }
    };

    AdvancedMessageListener aml = new AdvancedMessageListener() {
        @Override
        public void regularMessageReceived(SpreadMessage spreadMessage) {
            return;
        }

        @Override
        public void membershipMessageReceived(SpreadMessage spreadMessage) {
            System.out.println("recebi uma membership message");
            if(spreadMessage.getMembershipInfo().isCausedByLeave() ||
                    spreadMessage.getMembershipInfo().isCausedByDisconnect()){

                String aux = spreadMessage.getMembershipInfo().getLeft().toString();

                if(aux.startsWith(idSlave)){

                    System.out.println("Este slave saiu, inicia outro com este identificador: " + aux);
                }
                else{
                    System.out.println("Saiu um master! -> " + aux);
                }
            }
        }
    };

    public Master(String endereco, boolean r) {
        this.endereco = endereco;
        this.estadoRecuperado = r;
        this.descarta = !r;
        try {
            connection.connect(InetAddress.getByName("localhost"), 0, null, false, false);
            connectionGlobal.connect(InetAddress.getByName("localhost"), 0, null, false, true);
        } catch (SpreadException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        SpreadGroup group = new SpreadGroup();
        SpreadGroup groupGlobal = new SpreadGroup();
        try {
            group.join(connection, "master");
            groupGlobal.join(connectionGlobal,"global");
        } catch (SpreadException e) {
            e.printStackTrace();
        }
        ms = NettyMessagingService.builder().withAddress(Address.from(endereco)).build();

        ms.start();

        this.registaHandlers();
        connection.add(bml);
        connectionGlobal.add(aml);

        if(estadoRecuperado){
            System.out.println("Não vou recuperar estado");
        }
        else{
            System.out.println("Vou recuperar estado");

            PedidoEstadoMaster pem = new PedidoEstadoMaster(UUID.randomUUID().toString());
            SpreadMessage sm = new SpreadMessage();
            sm.setData(s.encode(pem));
            sm.addGroup("master");
            sm.setReliable();
            sm.setAgreed();
            try {
                connection.multicast(sm);
            } catch (SpreadException e) {
                e.printStackTrace();
            }
        }
    }

    public void trataMensagem(Object o){
        if(o instanceof GetRequest){
            GetRequest gr = (GetRequest) o;
            KeysUniverse ku = new KeysUniverse(gr.key, gr.key);
            SlaveIdentifier slaveI = slaves.get(ku);
            ReplyMaster rm = new ReplyMaster(gr.id, slaveI.endereco, slaveI.keys, gr.key);
            ms.sendAsync(Address.from(gr.endereco), "getMaster", s.encode(rm));
        }
        else if(o instanceof PutRequest){
            PutRequest pr = (PutRequest) o;
            KeysUniverse ku = new KeysUniverse(pr.key, pr.key);
            SlaveIdentifier slave = slaves.get(ku);
            ReplyMaster rm = new ReplyMaster(pr.id, slave.endereco,slave.keys, pr.key);
            ms.sendAsync(Address.from(pr.endereco),"putMaster",s.encode(rm));
        }
        else if(o instanceof ScanRequest){
            ScanRequest srq = (ScanRequest) o;
            ScanReply sr = new ScanReply(srq.id, slaves);
            ms.sendAsync(Address.from(srq.endereco), "scanMaster", s.encode(sr));
            System.out.println("Enviado");
        }
        else if(o instanceof RemoveRequest){
            RemoveRequest rr = (RemoveRequest) o;
            KeysUniverse ku = new KeysUniverse(rr.key, rr.key);
            SlaveIdentifier slaveI = slaves.get(ku);
            ReplyMaster rm = new ReplyMaster(rr.id, slaveI.endereco, slaveI.keys, rr.key);

            ms.sendAsync(Address.from(rr.endereco),"removeMaster", s.encode(rm));
        }
        else if(o instanceof StartRequest){
            System.out.println("Recebi uma mensagem de start");
            start.add(idSlave+this.nSlaves);
            StartRequest sr = new StartRequest(idSlave+this.nSlaves++,null);

            ms.sendAsync(Address.from(((StartRequest)o).endereco), "startFirst",s.encode(sr)); //envia o id ao slave que entrou!

            if(start.size() > 2){
                //enviar o conjunto das chaves
                ArrayList<String> it = new ArrayList<>(start);
                int atual = 0;
                int size = it.size();
                long chunk = 50;
                String end = it.get(atual);
                HashMap<String,Integer> secundarios = new HashMap<>();
                for(int k = 0; k < fatorReplicacao; k++){
                    int indice = (atual + k + 1) % size;
                    secundarios.put((it.get(indice)),k+1);
                }

                for(int i=0; i < 9; i++){
                    long inicial = i*50;
                    long finall = (i+1)*50;

                    if(i == 8) finall = Long.MAX_VALUE;
                    KeysUniverse ku = new KeysUniverse(inicial,finall);
                    System.out.println("SI: " + secundarios);
                    slaves.put(ku,new SlaveIdentifier(end,ku,secundarios));

                    System.out.println("Vou mandar uma mensagem para o: " + end);
                    int idAtual = 0;
                    StartMessage sm = new StartMessage(ku,idAtual);
                    //ms.sendAsync(end,"start",s.encode(sm));

                    for(Map.Entry<String,Integer> sec: secundarios.entrySet()){
                        sm.id = sec.getValue();

                        SpreadMessage smsg = new SpreadMessage();
                        smsg.setData(s.encode(sm));
                        smsg.addGroup(sec.getKey());
                        smsg.setReliable();
                        try {
                            connection.multicast(smsg);
                        } catch (SpreadException e) {
                            e.printStackTrace();
                        }
                    }


                    if((i+1) % 3 == 0){
                        System.out.println(size);
                        System.out.println(atual);
                        atual = (atual+1) % size;
                        System.out.println(atual);
                        end = it.get(atual);

                        secundarios = new HashMap<>();
                        for(int k = 0; k < fatorReplicacao; k++){
                            int indice = (atual + k + 1) % size;
                            System.out.println(indice);
                            secundarios.put((it.get(indice)),k+1);
                        }
                        System.out.println(secundarios);
                    }
                }
            }

            for(Map.Entry<KeysUniverse,SlaveIdentifier> me : this.slaves.entrySet()){
                System.out.println(me);
            }
        }
        else if(o instanceof RestartRequest){
            RestartRequest rr = (RestartRequest)o;
            TreeSet<KeysUniverse> aux = this.keysSlaves.get(rr.id);
            HashMap<String,Integer> grupos = new HashMap<>();

            for(KeysUniverse ku: aux){
                SlaveIdentifier si = slaves.get(ku);

                if(ku != null){
                    if(si.endereco.equals(rr.id)){
                        grupos.put(ku.getGrupo(),0);
                    }
                    else{
                        grupos.put(ku.getGrupo(),si.secundarios.get(rr.id));
                    }
                }
                else{
                    System.out.println("SlaveID null!!!");
                }
            }

            RestartReply rp = new RestartReply(aux,grupos);
            ms.sendAsync(Address.from(rr.endereco),"restart",s.encode(rp));



        }
        else{

        }
    }

    private void trataFila(){
        for(Object o: fila){
            trataMensagem(o);
        }
    }

    private void registaHandlers(){

        ms.registerHandler("put",(a,m) -> {
            PutRequest pr = s.decode(m);
            KeysUniverse ku = new KeysUniverse(pr.key, pr.key);
            SlaveIdentifier slave = slaves.get(ku);
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
            System.out.println("Pedido scan");
            ScanReply sr = new ScanReply(s.decode(m), this.slaves);
            System.out.println("Scan reply criado, enviar para: " + o);
            ms.sendAsync(o, "scanMaster", s.encode(sr));
            System.out.println("Enviado");
        },ses);

        ms.registerHandler("start", (o,m) -> { //para já assumimos que só são feitos 3. depois ver como contornar este problema
            System.out.println("Recebi uma mensagem de start");
            start.add(o.toString());

            if(start.size() > 2){
                //enviar o conjunto das chaves
                Iterator<String> it = start.iterator();
                Address end = Address.from(it.next());
                long chunk = 50;
                for(int i=0; i < 9; i++){
                    long inicial = i*50;
                    long finall = (i+1)*50;

                    if(i == 8) finall = Long.MAX_VALUE;
                    KeysUniverse ku = new KeysUniverse(inicial,finall);
                    slaves.put(ku,new SlaveIdentifier(end.toString(),ku));

                    System.out.println("Vou mandar uma mensagem para o: " + end.toString());
                    System.out.println("PELO MS JA NAO VAI MANDAR MENSAGEM!!!!!!! DE START!!!!!");
                    /*String replyID = UUID.randomUUID().toString();
                    StartReply sr = new StartReply(replyID, ku);
                    ms.sendAsync(end,"start",s.encode(sr));
*/

                    if((i+1) % 3 == 0) end = Address.from(it.next());
                }
            }
        }, ses);
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

        String endereco = "localhost:1233" + args[0];

        /*KeysUniverse ku1 = new KeysUniverse(0, 100);
        KeysUniverse ku2 = new KeysUniverse(100, 200);
        KeysUniverse ku3 = new KeysUniverse(200, 300);

        SlaveIdentifier slave1 = new SlaveIdentifier("localhost:12341", ku1);
        SlaveIdentifier slave2 = new SlaveIdentifier("localhost:12342", ku2);
        SlaveIdentifier slave3 = new SlaveIdentifier("localhost:12343", ku3);

        slaves.put(ku1, slave1);
        slaves.put(ku2, slave2);
        slaves.put(ku3, slave3);*/

        System.out.println(args.length);
        Master m = new Master(endereco, args.length>1);

        //m.teste();



        /*while(true){
            try {
                Thread.sleep(10000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/

        /*String jsonString = "{'ola': 'mania', 'meu': mania}";
        ObjectMapper objectMapper = new ObjectMapper();

        SimpleFilterProvider filterProvider = new SimpleFilterProvider();
        filterProvider.addFilter("empFilter",
                SimpleBeanPropertyFilter.filterOutAllExcept("\"v1\""));



        objectMapper.setFilterProvider(filterProvider);
        try {
            JsonNode actualObj = objectMapper.readTree("{\"k1\":\"v1\"}");
            String s = objectMapper.writeValueAsString(actualObj);
            System.out.println(s);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }*/




    }
}
