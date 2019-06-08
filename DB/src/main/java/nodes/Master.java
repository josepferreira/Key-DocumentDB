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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class  Master {

    public TreeMap<KeysUniverse,SlaveIdentifier> slaves = new TreeMap<>();
    public String endereco;
    ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();
    private HashSet<String> start = new HashSet<>();
    SpreadConnection connection = new SpreadConnection();

    private boolean estadoRecuperado;
    private boolean descarta;

    private ArrayList<Object> fila = new ArrayList<>();

    private HashSet<String> pedidosEstado = new HashSet<>();

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

    public Master(String endereco, boolean r) {
        this.endereco = endereco;
        this.estadoRecuperado = r;
        this.descarta = !r;
        try {
            connection.connect(InetAddress.getByName("localhost"), 0, null, false, false);
        } catch (SpreadException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        SpreadGroup group = new SpreadGroup();
        try {
            group.join(connection, "master");
        } catch (SpreadException e) {
            e.printStackTrace();
        }
        ms = NettyMessagingService.builder().withAddress(Address.from(endereco)).build();

        ms.start();

        this.registaHandlers();
        connection.add(bml);

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
            StartRequest sr = (StartRequest) o;
            start.add(sr.endereco);

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

                    //Necessario acrescentar o i para diferenciar os ids dos chunks
                    String replyID = sr.id + i;
                    StartReply startrep = new StartReply(replyID, ku);
                    ms.sendAsync(end,"start",s.encode(startrep));

                    if((i+1) % 3 == 0 && it.hasNext()) end = Address.from(it.next());
                }
            }
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
