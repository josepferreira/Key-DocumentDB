package nodes;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.Container;
import io.atomix.cluster.messaging.*;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import messages.*;
import spread.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

class ParSaiEntra{
    public String primario;
    public ArrayList<String> secundarios;

    public ParSaiEntra(String primario, ArrayList<String> secundarios) {
        this.primario = primario;
        this.secundarios = secundarios;
    }
}

public class  Master {

    public final DockerClient dockerClient = DefaultDockerClient
            .fromEnv()
            .build();

    public final String idSlave = "slave";


    public String endereco;
    ManagedMessagingService ms;
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    Serializer s = SerializerProtocol.newSerializer();
    SpreadConnection connection = new SpreadConnection();
    SpreadConnection connectionGlobal = new SpreadConnection();

    public final int fatorReplicacao = Config.fatorReplicacao;


    private ArrayList<Object> fila = new ArrayList<>();
    private HashSet<String> pedidosEstado = new HashSet<>();
    private boolean estadoRecuperado;
    private boolean descarta;

    public int nSlavesMinimo = Config.nSlaves;
    public int nConjuntos = Config.nConjuntos;

    //estado partilhado
    public int nSlaves = 0;
    public TreeMap<KeysUniverse,SlaveIdentifier> slaves = new TreeMap<>();
    private HashSet<String> start = new HashSet<>();
    private HashMap<String, TreeSet<KeysUniverse>> keysSlaves = new HashMap<>();
    private int ultimoID = 0;
    private HashMap<String, String> dockers = new HashMap<>();
    //Para a monitorizacao
    public boolean balanceamentoCarga = false;
    public HashMap<String,InfoMonitorizacao> infoSlaves = new HashMap<>();
    public HashMap<String,HashSet<KeysUniverse>> esperaEntra = new HashMap<>();
    public HashMap<String,HashSet<KeysUniverse>> esperaSai = new HashMap<>();


    BasicMessageListener bml = new BasicMessageListener() {
        @Override
        public void messageReceived(SpreadMessage spreadMessage) {
            byte[] msg = spreadMessage.getData();
            Object o = s.decode(msg);

            if(descarta){
                if(o instanceof PedidoEstado){
                    descarta = false;

                }
            }
            else{
                if(!estadoRecuperado){
                    if(o instanceof EstadoMaster){
                        //recupera estado
                        EstadoMaster em = (EstadoMaster)o;
                        if(!pedidosEstado.contains(em.id)){
                            pedidosEstado.add(em.id);
                            start = em.start;
                            nSlaves = em.nSlaves;
                            slaves = em.slaves;
                            keysSlaves = em.keysSlaves;
                            ultimoID = em.ultimoID;
                            dockers = em.dockers;
                            balanceamentoCarga = em.balanceamentoCarga;
                            infoSlaves = em.infoSlaves;
                            esperaEntra = em.esperaEntra;
                            esperaSai = em.esperaSai;
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
                        EstadoMaster em = new EstadoMaster(((PedidoEstado)o).id, nSlaves, slaves, start, keysSlaves, ultimoID, dockers, balanceamentoCarga, infoSlaves, esperaEntra, esperaSai);
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
            if(spreadMessage.getMembershipInfo().isCausedByLeave() ||
                    spreadMessage.getMembershipInfo().isCausedByDisconnect()) {

                String aux = spreadMessage.getMembershipInfo().getLeft().toString().split("#")[1];

                if (estadoRecuperado) {

                    if (aux.startsWith(idSlave)) {


                        TreeSet<KeysUniverse> tk = keysSlaves.get(aux);
                        if (tk != null) {
                            for (KeysUniverse ku : tk) {
                                SlaveIdentifier si = slaves.get(ku);

                                if (si == null) {
                                    System.out.println("Slave identifier é nulo no slave q saiu!");
                                } else {
                                    si.sai(aux);
                                }
                            }
                        } else {
                            System.out.println("Saiu um slave mas n aparece nos registos!!!");
                        }
                    } else {
                        System.out.println("Saiu um master! ID: " + aux);
                    }
                }
            }else {
                fila.add(spreadMessage);
            }
        }
    };

    public Master(String endereco, boolean r) throws DockerCertificateException {
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
//            System.out.println("Não vou recuperar estado");
        }
        else{

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
        }
        else if(o instanceof RemoveRequest){
            RemoveRequest rr = (RemoveRequest) o;
            KeysUniverse ku = new KeysUniverse(rr.key, rr.key);
            SlaveIdentifier slaveI = slaves.get(ku);
            ReplyMaster rm = new ReplyMaster(rr.id, slaveI, rr.key);

            ms.sendAsync(Address.from(rr.endereco),"removeMaster", s.encode(rm));
        }
        else if(o instanceof StartRequest){
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
                    slaves.put(ku,new SlaveIdentifier(end,ku,secundarios));

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
                        atual = (atual+1) % size;
                        end = it.get(atual);

                        secundarios = new HashMap<>();
                        for(int k = 0; k < fatorReplicacao; k++){
                            int indice = (atual + k + 1) % size;
                            secundarios.put((it.get(indice)),k+1);
                        }
                    }
                }
            }
/*
            for(Map.Entry<KeysUniverse,SlaveIdentifier> me : this.slaves.entrySet()){
                System.out.println(me);
            }*/
        }
        else if(o instanceof RestartRequest){
            RestartRequest rr = (RestartRequest)o;
            TreeSet<KeysUniverse> aux = this.keysSlaves.get(rr.id);
            TreeMap<KeysUniverse,String> grupos = new TreeMap<>();
            HashMap<KeysUniverse,Boolean> podeEntrar = new HashMap<>();

            for(KeysUniverse ku: aux){
                SlaveIdentifier si = slaves.get(ku);

                if(si != null){
                    si.entra(rr.id,rr.endereco);
                    if(si.idPrimario.equals(rr.id)){
                        grupos.put(ku,0+"");
                        boolean pode = true;
                        if(balanceamentoCarga){
                            HashSet<KeysUniverse> univ = esperaEntra.get(rr.id);
                            if(univ != null && univ.contains(ku)){
                                pode = false;
                            }
                        }
                        podeEntrar.put(ku,pode);
                    }
                    else{
                        grupos.put(ku,si.secundarios.get(rr.id).id+"");
                        podeEntrar.put(ku,true);
                    }
                }
                else{
//                    System.out.println("SlaveID null!!!");
                }
            }


            RestartReply rp = new RestartReply(grupos,podeEntrar,rr.id);
            ms.sendAsync(Address.from(rr.endereco),"restart",s.encode(rp));



        }
        else if(o instanceof InfoMonitorizacao){

            InfoMonitorizacao im = (InfoMonitorizacao) o;
            infoSlaves.put(im.id,im);

            if(!balanceamentoCarga){

                if((im.cpu > Config.cpuMax ||
                        im.memoria < Config.memMin)
                 && nSlaves < nConjuntos){
                    System.out.println("Tem demasiado cpu ou pouca memória por isso é necessário aumentar o número de slaves!");
                    balanceamentoCarga = true;
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
        else if(o instanceof LeaveGroupsReply){
            LeaveGroupsReply lgr = (LeaveGroupsReply)o;

            HashSet<KeysUniverse> espera = this.esperaSai.get(lgr.id);
            if(espera == null){
                System.out.println("Deu null nos q esperam ao receber resposta de leave!");
            }
            else{
                espera.removeAll(lgr.primarios);
                HashMap<String,HashSet<KeysUniverse>> enviaJoin = new HashMap<>();
                for(KeysUniverse ku: lgr.primarios){
                    for(Map.Entry<String,HashSet<KeysUniverse>> entry: esperaEntra.entrySet()){
                        if(entry.getValue().contains(ku)){
                            HashSet<KeysUniverse> aux = enviaJoin.get(entry.getKey());
                            if(aux == null){
                                aux = new HashSet<>();
                            }
                            aux.add(ku);
                            enviaJoin.put(entry.getKey(),aux);
                        }
                    }
                }

                for(Map.Entry<String,HashSet<KeysUniverse>> entry: enviaJoin.entrySet()){
                    String id = ""+ultimoID++;
                    JoinGroup jg = new JoinGroup(entry.getValue(),id);

                    HashSet<KeysUniverse> aux = esperaEntra.get(entry.getKey());
                    if(aux == null){
                        System.out.println("Era null!!!");
                    }
                    else{
                        aux.removeAll(entry.getValue());
                        if(aux.isEmpty()){
                            esperaEntra.remove(entry.getKey());
                        }
                        else{
                            esperaEntra.put(entry.getKey(),aux);
                        }
                    }


                    SpreadMessage sm = new SpreadMessage();
                    sm.setData(s.encode(jg));
                    sm.addGroup(entry.getKey());
                    sm.setReliable();
                    try {
                        connection.multicast(sm);
                    } catch (SpreadException e) {
                        e.printStackTrace();
                    }

                }

                if(esperaEntra.isEmpty()){
                    esperaSai.clear();
                    balanceamentoCarga = false;
                }



            }
        }else if(o instanceof SpreadMessage) {
            SpreadMessage spreadMessage = (SpreadMessage) o;

            if (spreadMessage.getMembershipInfo().isCausedByLeave() ||
                    spreadMessage.getMembershipInfo().isCausedByDisconnect()) {


                String aux = spreadMessage.getMembershipInfo().getLeft().toString().split("#")[1];

                if (aux.startsWith(idSlave)) {


                    TreeSet<KeysUniverse> tk = keysSlaves.get(aux);
                    if (tk != null) {
                        for (KeysUniverse ku : tk) {
                            SlaveIdentifier si = slaves.get(ku);

                            if (si == null) {

                                System.out.println("Slave identifier é nulo no slave q saiu!");
                            } else {
                                si.sai(aux);
                            }
                        }
                    } else {
                        System.out.println("Saiu um slave mas n aparece nos registos!!!");
                    }
                } else {
                    System.out.println("Saiu um master! ID: " + aux);
                }
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

    private ParPrimarioSecundario escolheParRemove(String slA, String slN){
        TreeSet<KeysUniverse> univ = keysSlaves.get(slA);
        TreeSet<KeysUniverse> chavesN = keysSlaves.get(slN);

        KeysUniverse escolhido = null;
        for(KeysUniverse ku: univ){
            if(!chavesN.contains(ku)){
                escolhido = ku;
                break;
            }
        }

        SlaveIdentifier si = slaves.get(escolhido);
        int nSec = 0;
        KeysUniverse primario = null;
        HashSet<KeysUniverse> secundarios = new HashSet<>();
        if(!si.primarioID().equals(slA)){
            //era secundario, escolher primario
            nSec = 1;
            secundarios.add(escolhido);
            for(KeysUniverse ku : univ){

                if(!chavesN.contains(ku)){
                    SlaveIdentifier siA = slaves.get(ku);

                    if(siA.primarioID().equals(slA)){
                        primario = ku;
                        break;
                    }
                }
            }
        }
        else{
            primario = escolhido;
        }

        //escolher os restantes secundarios
        for(KeysUniverse ku : univ){
            if(nSec >= fatorReplicacao) {
                break;
            }

            if(!chavesN.contains(ku)){
                SlaveIdentifier siA = slaves.get(ku);

                if(!siA.primarioID().equals(slA)){
                    secundarios.add(ku);
                    nSec++;
                }
            }
        }

        univ.remove(primario);
        univ.removeAll(secundarios);
        chavesN.add(primario);
        chavesN.addAll(secundarios);

        ParPrimarioSecundario pps = new ParPrimarioSecundario(primario,secundarios);
        return pps;
    }

    private void diminuiSlaves(String slave){
        nSlaves--;

        int quantosConjuntos = primarios(slave);

        int maxConjunto = nConjuntos / nSlaves;
        int quantosMax = nConjuntos % nSlaves;

        if(quantosMax != 0){
            maxConjunto++;
        }
        int atualMax = 0;

        HashMap<String,Integer> nPrimarios = new HashMap<>();
        ArrayList<String> slavesL = new ArrayList<>(keysSlaves.keySet());
        slavesL.remove(slave);

        for(String sA: slavesL){
            int aux = primarios(sA);
            if(aux >= maxConjunto){
                atualMax++;
            }
            nPrimarios.put(sA,aux);
        }

        int indice = 0;

        HashMap<String,ArrayList<ParPrimarioSecundario>> pares = new HashMap<>();

        while(quantosConjuntos > 0){

            int comparacao = maxConjunto;

            if(quantosMax <= atualMax){
                comparacao -= 1;
            }

            String slA = slavesL.get(indice);

            int prim = nPrimarios.get(slA);

            if(prim >= comparacao){
                System.out.println("Já tem os primários todos");
            }
            else{
                //escolher primarios e secundarios

                ParPrimarioSecundario pps = escolheParRemove(slave,slA);

                ArrayList<ParPrimarioSecundario> alp = pares.get(slA);

                if(alp == null){
                    alp = new ArrayList<>();
                }

                alp.add(pps);

                pares.put(slA,alp);

                quantosConjuntos--;
                nPrimarios.put(slA,prim+1);
                if((prim+1) >= maxConjunto){
                    atualMax++;
                }
            }
        }

    }

    private void atualizaEstadoSaem(HashMap<String,ArrayList<ParPrimarioSecundario>> saem){

        for(Map.Entry<String,ArrayList<ParPrimarioSecundario>> entry : saem.entrySet()){
            String idS = entry.getKey();
            ArrayList<ParPrimarioSecundario> pares = entry.getValue();
            HashSet<KeysUniverse> espera = new HashSet<>();
            TreeSet<KeysUniverse> keys = keysSlaves.get(idS);

            for(ParPrimarioSecundario par: pares){
                //adicionar às suas keys
                keys.remove(par.primario);
                keys.removeAll(par.secundarios);
                //atualizar os slaves identifiers
                SlaveIdentifier si = slaves.get(par.primario);

                si.ativo = false;

                for(KeysUniverse aux: par.secundarios){
                    si.removeSecundario(idS);
                }
                //adicionar espera pelo primario
                espera.add(par.primario);
            }

            esperaSai.put(idS,espera);


            String idM = ""+ultimoID++;
            LeaveGroups lg = new LeaveGroups(pares,idM);

            SpreadMessage sm = new SpreadMessage();
            sm.setData(s.encode(lg));
            sm.addGroup(idS);
            sm.setReliable();
            try {
                connection.multicast(sm);
            } catch (SpreadException e) {
                e.printStackTrace();
            }


        }
    }

    private void atualizaEstadoEntra(ArrayList<ParPrimarioSecundario> pares, String slave){

        start.add(slave);
        TreeSet<KeysUniverse> keys = new TreeSet<KeysUniverse>();

        HashSet<KeysUniverse> espera = new HashSet<>();

        for(ParPrimarioSecundario par: pares){
            //adicionar às suas keys
            keys.add(par.primario);
            keys.addAll(par.secundarios);
            //atualizar os slaves identifiers
            SlaveIdentifier si = slaves.get(par.primario);

            si.idPrimario = slave;
            si.endereco = null;
            si.ativo = false;

            for(KeysUniverse aux: par.secundarios){
                SlaveIdentifier novoSI = slaves.get(aux);
                novoSI.addSecundario(slave);
            }
            //adicionar espera pelo primario
            espera.add(par.primario);
        }

        keysSlaves.put(slave,keys);
        esperaEntra.put(slave,espera);

        //inicializar slave
//        criaDocker(slave);

    }

    private void aumentaSlaves(String slave){
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

        HashMap<String,ArrayList<ParPrimarioSecundario>> saem = new HashMap<>();
        ArrayList<ParPrimarioSecundario> asd = new ArrayList<>();
        asd.add(ps);
        saem.put(slave,asd);

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

                ArrayList<ParPrimarioSecundario> auxPares = saem.get(sl);
                if(auxPares == null){
                    auxPares = new ArrayList<>();
                }
                auxPares.add(novoPS);
                saem.put(sl,auxPares);

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
        /*System.out.println("Fim aumento!");
        for(ParPrimarioSecundario par : pares){
            System.out.println(par);
        }

        System.out.println();
        for(Map.Entry<String,TreeSet<KeysUniverse>> ent: keysSlaves.entrySet()){
            System.out.println(ent);
        }*/

        apresentaConfiguracao();

        atualizaEstadoSaem(saem);
        atualizaEstadoEntra(pares,this.idSlave+nSlaves);

    }

    private void apresentaConfiguracao(){
        HashMap<String,ArrayList<KeysUniverse>> primarios = new HashMap<>();
        HashMap<String,ArrayList<KeysUniverse>> secundarios = new HashMap<>();

        for(Map.Entry<KeysUniverse,SlaveIdentifier> entry: this.slaves.entrySet()){
            SlaveIdentifier si = entry.getValue();
            ArrayList<KeysUniverse> aux = primarios.get(si.idPrimario);
            if(aux == null){
                aux = new ArrayList<>();
            }
            aux.add(entry.getKey());
            primarios.put(si.idPrimario,aux);

            for(Map.Entry<String,Secundario> sec: si.secundarios.entrySet()){
                ArrayList<KeysUniverse> aux2 = secundarios.get(sec.getKey());
                if(aux2 == null){
                    aux2 = new ArrayList<>();
                }
                aux2.add(entry.getKey());
                secundarios.put(sec.getKey(),aux2);
            }
        }

//        for(String sl : primarios.keySet()){
//
//            System.out.println("Slave: " + sl);
//            System.out.println("Primário: " + primarios.get(sl));
//            System.out.println("Secundário: " + secundarios.get(sl));
//
//            System.out.println("----");
//        }

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
        System.out.println("Iniciar slaves: " + fatorReplicacao);

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

        ArrayList<byte[]> divisoesConj = Config.conjuntosChave();
        for(int i=0; i < nConjuntos; i++) {
            secundarios = new HashMap<>();
            for(int k = 0, tentativas = 0; tentativas < fatorReplicacao; k++, tentativas++){
                int indice = (primeiroSec + k) % size;
                if(indice == atual){
                    tentativas--;
                }
                else{
                    secundarios.put((it.get(indice)),k+1);
                }
            }

            byte[] inicial = divisoesConj.get(i);
            byte[] finall = divisoesConj.get(i+1);

            if (i == (nConjuntos-1)) finall = divisoesConj.get(divisoesConj.size()-1);
            KeysUniverse ku = new KeysUniverse(inicial, finall);
            SlaveIdentifier si = new SlaveIdentifier(end, ku, secundarios);
            slaves.put(ku,si);

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


        apresentaConfiguracao();

        /*for(String slaveString: start){
            criaDocker(slaveString);
        }*/

    }

    private void eliminaContainer(String nameContainer){
        String idContainer = this.dockers.remove(nameContainer);

        if(idContainer == null)
            System.out.println("container null");
        else{
            try {
                this.dockerClient.stopContainer(idContainer, 0);
            } catch (DockerException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                this.dockerClient.removeContainer(idContainer);
            } catch (DockerException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private String executaShellCreateContainer(String nameContainer){

        String ret = null;
        try {
            Process process = Runtime.getRuntime().exec("docker run -d --network host --name " + nameContainer + " lei/testeslave " + nameContainer);//"docker", "run", "--network", "host", "--name", nameContainer, nameImage, "1", "2");
            //Process process = processBuilder.start();

            StringBuilder output = new StringBuilder();

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line + "\n");
            }

            int exitVal = process.waitFor();
            if (exitVal == 0) {
                return output.toString();
                //System.exit(0);
            } else {
                List<Container> a = dockerClient.listContainers();
                String idContainer = null;
                for(Container c: a){
                    if(c.names().asList().get(0).equals("/" + nameContainer)) {
                        idContainer = c.id();
                    }
                }
                if(idContainer == null)
                    return null;
                if(this.dockerClient.inspectContainer(idContainer).state().status().equals("exited")){
                    this.dockerClient.removeContainer(idContainer);
                    return null;
                }else{
                    return idContainer;
                }
                //abnormal...
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (DockerException e) {
            e.printStackTrace();
        }

        return ret;
    }

    private void criaDocker(String nameContainer){

        String espera = null;
        // -- Linux --
        // Run a shell command2
        String idContainer = null;
        while(idContainer == null){
            idContainer = executaShellCreateContainer(nameContainer);
        }

        this.dockers.put(nameContainer, idContainer);
    }

    public static void main(String[] args) throws DockerCertificateException {

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
