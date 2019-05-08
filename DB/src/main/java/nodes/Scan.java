package nodes;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import messages.ScanRequest;
import messages.SlaveScanReply;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public class Scan {
    String id;
    long ultimoVisto  = -1;
    KeysUniverse ultimoUniverso = null;
    ArrayList<Predicate<JSONObject>> filtros;
    HashMap<Boolean,ArrayList<String>> projecoes;
    int nrMaximo;
    private TreeMap<KeysUniverse,SlaveIdentifier> cache;
    private Serializer s = SerializerProtocol.newSerializer();
    private int tamanhoAtual = 0;
    private CompletableFuture<Void> cf;
    public LinkedHashMap<Long,JSONObject> docs = new LinkedHashMap<>();
    private boolean existemMais = true;
    private ManagedMessagingService ms;
    private CompletableFuture<Void> esperaCache = new CompletableFuture<>();

    public Scan(String id, ArrayList<Predicate<JSONObject>> filtros, HashMap<Boolean, ArrayList<String>> projecoes, int nrMaximo,
                ManagedMessagingService ms) {
        this.id = id;
        this.filtros = filtros;
        this.projecoes = projecoes;
        this.nrMaximo = nrMaximo;
        this.ms = ms;
    }

    public void registaCache(TreeMap<KeysUniverse, SlaveIdentifier> cache){
        this.cache = cache;
        esperaCache.complete(null);
    }

    public CompletableFuture<Void> getMore() throws Exception{
        System.out.println("GET MORE");
        cf = new CompletableFuture<>();
        if(!existemMais){
            System.out.println("Lançando excepção!!!");
            throw new Exception("Não existem mais");
        }
        esperaCache.thenAccept(a ->{
            if(ultimoVisto == -1){
                //n foi buscar nenhuma ainda
                String endereco = cache.get(cache.firstKey()).endereco; //assumindo que existe em cache
                System.out.println(cache.firstKey());
                System.out.println("End: " + endereco);
                ScanRequest sr = new ScanRequest(id,filtros,projecoes,cache.firstKey(),nrMaximo,-1);
                try{
                    s.encode(sr);
                }
                catch (Exception e){
                    System.out.println(e);
                }
                ms.sendAsync(Address.from(endereco),"scan",s.encode(sr));
            }
            else{
                //senao vai buscar a ultima ao universo atual
                //n foi buscar nenhuma ainda
                String endereco = cache.get(ultimoUniverso).endereco; //assumindo que existe em cache
                ScanRequest sr = new ScanRequest(id,filtros,projecoes,ultimoUniverso,nrMaximo,ultimoVisto);
                docs = new LinkedHashMap<>();
                tamanhoAtual = 0;
                ms.sendAsync(Address.from(endereco),"scan",s.encode(sr));

            }
        });
        return cf;
    }

    public void registaResposta(SlaveScanReply ssr){
        int tamanho = ssr.docs.size();
        tamanhoAtual += tamanho;
        docs.putAll(ssr.docs);
        System.out.println("Tamanho atual: " + tamanho);
        if(tamanhoAtual == nrMaximo){
            ultimoVisto = ssr.ultimaChave;
            ultimoUniverso = ssr.universe;

                //vamos ter de responder
            cf.complete(null);
        }
        else{
            //pode pedir mais ao próximo
            KeysUniverse proximo = cache.higherKey(ultimoUniverso);
            System.out.println("Proximo: " + proximo);
            if(proximo == null){
                existemMais = false;
                cf.complete(null);
            }
            else {
                String endereco = cache.get(proximo).endereco; //assumindo que existe em cache
                ultimoUniverso = proximo;
                ScanRequest sr = new ScanRequest(id, filtros, projecoes, ultimoUniverso, nrMaximo - tamanhoAtual, -1);
                ms.sendAsync(Address.from(endereco), "scan", s.encode(sr));
                System.out.println("Atualizado: " + ultimoUniverso);
            }
        }
    }


}
