package nodes;

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

class ParPrimarioSecundario{
    public KeysUniverse primario;
    public HashSet<KeysUniverse> secundarios;

    public ParPrimarioSecundario(KeysUniverse primario, HashSet<KeysUniverse> secundarios) {
        this.primario = primario;
        this.secundarios = secundarios;
    }

    @Override
    public String toString() {
        return "ParPrimarioSecundario{" +
                "primario=" + primario +
                ", secundarios=" + secundarios +
                '}';
    }
}

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

    public int nSlavesMinimo = Config.nSlaves;
    public int nConjuntos = Config.nConjuntos;

    //Para a monitorizacao
    public boolean balanceamentoCarga = false;
    public HashMap<String,InfoMonitorizacao> infoSlaves = new HashMap<>();

    BasicMessageListener bml = new BasicMessageListener() {
        @Override
        public void messageReceived(SpreadMessage spreadMessage) {
            byte[] msg = spreadMessage.getData();
            Object o = s.decode(msg);
            System.out.println("GROUP:" + spreadMessage.getSender());

            if(descarta){
                if(o instanceof PedidoEstado){
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
                            nSlaves = em.nSlaves;
                            keysSlaves.putAll(em.keysSlaves);
                            estadoRecuperado = true;
                            trataFila();
                        }
                    }
                    else{
                        fila.add(o);
                    }
                }
                else{
                    if(o instanceof PedidoEstado){
                        System.out.println("Vou responder ao pedido de estado");
                        EstadoMaster em = new EstadoMaster(((PedidoEstado)o).id,nSlaves,slaves,start,keysSlaves);
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

                String aux = spreadMessage.getMembershipInfo().getLeft().toString().split("#")[1];

                if(aux.startsWith(idSlave)){

                    System.out.println("Este slave saiu, inicia outro com este identificador: " + aux);

                    TreeSet<KeysUniverse> tk = keysSlaves.get(aux);
                    if(tk != null){
                        for(KeysUniverse ku: tk){
                            SlaveIdentifier si = slaves.get(ku);

                            if(si == null){
                                System.out.println("Slave identifier é nulo no slave q saiu!");
                            }
                            else{
                                si.sai(aux);
                            }
                        }
                    }
                    else {
                        System.out.println("Saiu um slave mas n aparece nos registos!!!");
                    }
                }
                else{
                    System.out.println("Saiu um master! ID: " + aux);
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

        connection.add(bml);
        connectionGlobal.add(aml);

        if(estadoRecuperado){
            System.out.println("Não vou recuperar estado");
        }
        else{
            System.out.println("Vou recuperar estado");

            PedidoEstado pem = new PedidoEstado(UUID.randomUUID().toString());
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

        iniciaSlaves(nSlavesMinimo,nConjuntos);
    }

    public void trataMensagem(Object o){
        if(o instanceof GetRequest){
            GetRequest gr = (GetRequest) o;
            KeysUniverse ku = new KeysUniverse(gr.key, gr.key);
            SlaveIdentifier slaveI = slaves.get(ku);
            ReplyMaster rm = new ReplyMaster(gr.id, slaveI, gr.key);
            ms.sendAsync(Address.from(gr.endereco), "getMaster", s.encode(rm));
        }
        else if(o instanceof PutRequest){
            PutRequest pr = (PutRequest) o;
            KeysUniverse ku = new KeysUniverse(pr.key, pr.key);
            SlaveIdentifier slave = slaves.get(ku);
            ReplyMaster rm = new ReplyMaster(pr.id, slave, pr.key);
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
            ReplyMaster rm = new ReplyMaster(rr.id, slaveI, rr.key);

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
            System.out.println("VER CHAVES!!!!");
            System.out.println(this.keysSlaves);
            System.out.println(rr.id);
            TreeMap<KeysUniverse,String> grupos = new TreeMap<>();

            for(KeysUniverse ku: aux){
                SlaveIdentifier si = slaves.get(ku);

                if(si != null){
                    si.entra(rr.id,rr.endereco);
                    if(si.idPrimario.equals(rr.id)){
                        grupos.put(ku,0+"");
                    }
                    else{
                        grupos.put(ku,si.secundarios.get(rr.id).id+"");
                    }
                }
                else{
                    System.out.println("SlaveID null!!!");
                }
            }


            RestartReply rp = new RestartReply(rr.id ,grupos);
            ms.sendAsync(Address.from(rr.endereco),"restart",s.encode(rp));



        }
        else if(o instanceof InfoMonitorizacao){

            InfoMonitorizacao im = (InfoMonitorizacao) o;
            infoSlaves.put(im.id,im);

            if(!balanceamentoCarga){
                System.out.println("Vou considerar a mensagem de balanceamento de carga");

                if((im.cpu > Config.cpuMax ||
                        im.memoria < Config.memMin)
                 && nSlaves < nConjuntos){
                    System.out.println("Tem demasiado cpu ou pouca memória por isso é necessário aumentar o número de slaves!");
                    aumentaSlaves(im.id);
                }
                else{
                    int escritas = 0;
                    int leituras = 0;
                    for(ParEscritaLeitura pel: im.operacoes.values()){
                        escritas += pel.escritas;
                        leituras = pel.leituras;
                    }

                    double valor = ((0.3 * leituras) + (0.7 * escritas)) / Config.periodoTempo;

                    if(valor < Config.valorMin && nSlaves > nSlavesMinimo){
                        System.out.println("Pedidos estão subcarregados, diminuir conjunto de slaves!");
                    }
                }


            }
            else{
                System.out.println("Recebi mensagem de monitorização mas esta a decorrer um balanceamento pelo que vai ser ignorada esta mensagem!");
            }
        }
        else{
            System.out.println("RECEBI ALGO QUE SUPOSTAMENTE EU NÃO DEVERIA DE RECEBER ... HACKERMAN :o " + o.getClass());
        }
    }

    private void trataFila(){
        for(Object o: fila){
            trataMensagem(o);
        }
    }

    private KeysUniverse maximaUtilizacao(InfoMonitorizacao im){
        KeysUniverse ku = null;
        double valor = Double.MIN_VALUE;

        for(Map.Entry<KeysUniverse,ParEscritaLeitura> entry: im.operacoes.entrySet()){

            double valorAux = ((0.3 * entry.getValue().leituras) + (0.7 * entry.getValue().escritas));
            valorAux = valorAux / (double) Config.periodoTempo;

            if(Double.compare(valor,valorAux) > 0){
                ku = entry.getKey();
                valor = valorAux;
            }
        }


        return ku;
    }

    private KeysUniverse minimaUtilizacao(InfoMonitorizacao im, HashSet<KeysUniverse> pE, HashSet<KeysUniverse> sE){
        KeysUniverse ku = null;
        double valor = Double.MIN_VALUE;

        for(Map.Entry<KeysUniverse,ParEscritaLeitura> entry: im.operacoes.entrySet()){
            if( !( pE.contains(entry.getKey()) || sE.contains(entry.getKey()) ) ) {


                double valorAux = ((0.3 * entry.getValue().leituras) + (0.7 * entry.getValue().escritas));
                valorAux = valorAux / (double) Config.periodoTempo;

                if(Double.compare(valor,valorAux) > 0){
                    ku = entry.getKey();
                    valor = valorAux;
                }

            }
        }


        return ku;
    }

    public ParPrimarioSecundario selecionaPar(String slave, KeysUniverse ku,
                                              HashSet<KeysUniverse> primariosEscolhidos, HashSet<KeysUniverse> secundariosEscolhidos){
        SlaveIdentifier si = slaves.get(ku);
        HashSet<KeysUniverse> hs = new HashSet<KeysUniverse>();

        int quantos = 0;
        KeysUniverse primario = null;
        if(si.primarioID().equals(slave)){
            primario = ku;
        }
        else{
            hs.add(ku);
            quantos++;
            for(KeysUniverse aux: this.keysSlaves.get(slave)){
                if(!(primariosEscolhidos.contains(aux) || secundariosEscolhidos.contains(aux))){
                    if(slaves.get(aux).primarioID().equals(slave)){
                        //e primario
                        primario = aux;
                        break;
                    }
                }
            }
        }

        for(int i = quantos; i < fatorReplicacao; i++){
            for(KeysUniverse aux: this.keysSlaves.get(slave)){
                if(!(primariosEscolhidos.contains(aux) || secundariosEscolhidos.contains(aux))){
                    if(!slaves.get(aux).primarioID().equals(slave)){
                        //e secundario
                        hs.add(aux);
                    }
                }
            }
        }

        ParPrimarioSecundario ps = new ParPrimarioSecundario(primario,hs);
        return ps;
    }


    private void aumentaSlaves(String slave){
        System.out.println("Inicio aumento");
        int quantosConjuntos = nConjuntos / (++nSlaves);

        InfoMonitorizacao im = infoSlaves.get(slave);
        KeysUniverse ku = maximaUtilizacao(im);

        HashSet<KeysUniverse> primariosEscolhidos = new HashSet<>();
        HashSet<KeysUniverse> secundariosEscolhidos = new HashSet<KeysUniverse>();
        ArrayList<ParPrimarioSecundario> pares = new ArrayList<>();
        ParPrimarioSecundario ps = selecionaPar(slave,ku,primariosEscolhidos,secundariosEscolhidos);
        pares.add(ps);
        primariosEscolhidos.add(ps.primario);
        secundariosEscolhidos.addAll(ps.secundarios);

        int quantosMax = nConjuntos % nSlaves;
        int maxSlave = quantosConjuntos;
        if(quantosMax == 0 || (quantosMax == (nSlaves-1))){
            quantosMax = nSlaves-1;
            if(quantosMax == (nSlaves-1)){
                maxSlave += 1;
            }
        }
        else{
            quantosMax = nSlaves - 1 - quantosMax;
        }

        ArrayList<String> slavesL = new ArrayList<>(keysSlaves.keySet());
        HashMap<String,Integer> nPrimarios = new HashMap<>();

        for(String sl: slavesL){
            nPrimarios.put(sl,primarios(sl));
        }

        int nPA = nPrimarios.get(slave);
        nPrimarios.put(slave,nPA-1);


        int atual = slavesL.indexOf(slave);
        int auxMax = 0;

        if((nPA - 1) == maxSlave){
            auxMax = 1;
        }

        while(primariosEscolhidos.size() < quantosConjuntos){

            atual = (atual + 1) % slavesL.size();

            String sl = slavesL.get(atual);
            System.out.println("SLAVE: " + sl);

            int aux = nPrimarios.get(sl);

            int comparacao = maxSlave;
            if(auxMax >= quantosMax){
                comparacao = comparacao + 1;
            }

            if(aux > comparacao){
                //ir buscar conjunto de chaves com menos carga
                KeysUniverse novo = minimaUtilizacao(infoSlaves.get(sl),primariosEscolhidos,secundariosEscolhidos);
                //ir buscar par
                ParPrimarioSecundario novoPS = selecionaPar(sl,novo,primariosEscolhidos,secundariosEscolhidos);
                pares.add(novoPS);
                //adcionar ao primario
                primariosEscolhidos.add(novoPS.primario);
                //adicionar ao secundario
                secundariosEscolhidos.addAll(novoPS.secundarios);
                nPrimarios.put(sl,aux-1);
                if((aux-1) == maxSlave){
                    auxMax++;
                }
            }

        }
        System.out.println("Fim aumento!");
        for(ParPrimarioSecundario par : pares){
            System.out.println(par);
        }

        System.out.println();
        for(Map.Entry<String,TreeSet<KeysUniverse>> ent: keysSlaves.entrySet()){
            System.out.println(ent);
        }

    }

    private Integer primarios(String sl) {

        int i = 0;

        for(KeysUniverse ku: keysSlaves.get(sl)){

            SlaveIdentifier si = slaves.get(ku);

            if(si.primarioID().equals(sl)){
                i++;
            }
        }

        return i;
    }

    private void adicionaChave(String id, KeysUniverse ku){
        TreeSet<KeysUniverse> tk = keysSlaves.get(id);
        tk.add(ku);
    }

    private void iniciaSlaves(int n, int nConjuntos){
        System.out.println("Iniciar slaves");

        for(int i = 0; i < n; i++){
            System.out.println("Inicia um slave com o identificador: " + idSlave+this.nSlaves);
            start.add(idSlave+this.nSlaves);
            keysSlaves.put(idSlave+this.nSlaves,new TreeSet<KeysUniverse>());
            this.nSlaves++;
        }


        //enviar o conjunto das chaves
        ArrayList<String> it = new ArrayList<>(start);
        int atual = 0;
        int size = it.size();
        int primeiroSec = (atual+1) % size;
        long chunk = 50;
        String end = it.get(atual);
        HashMap<String,Integer> secundarios;
        int divisao = nConjuntos / n;


        for(int i=0; i < nConjuntos; i++) {
            secundarios = new HashMap<>();
            for(int k = 0; k < fatorReplicacao; k++){
                int indice = (primeiroSec + k) % size;
                if(indice == atual){
                    System.out.println("Deu a volta e por o secundario igual ao primario, ver pq!");
                }
                secundarios.put((it.get(indice)),k+1);
            }

            long inicial = i * chunk;
            long finall = (i + 1) * chunk;

            if (i == (nConjuntos-1)) finall = Long.MAX_VALUE;
            KeysUniverse ku = new KeysUniverse(inicial, finall);
            slaves.put(ku, new SlaveIdentifier(end, ku, secundarios));

            adicionaChave(end,ku);
            for(String secA: secundarios.keySet()){
                adicionaChave(secA,ku);
            }


            if ((i + 1) % divisao == 0) {
                atual = (atual + 1) % size;
                end = it.get(atual);
                primeiroSec = (atual+1) % size;

            }
            else{
                primeiroSec = (primeiroSec+1) % size;
                if(primeiroSec == atual){
                    primeiroSec = (atual+1) % size;
                }
            }
        }

        for(Map.Entry<KeysUniverse,SlaveIdentifier> me : this.slaves.entrySet()){
            System.out.println(me);
        }

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
