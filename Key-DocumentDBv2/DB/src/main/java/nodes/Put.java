package nodes;

import messages.PutRequest;
import org.json.JSONObject;

import java.util.concurrent.CompletableFuture;

public class Put {
    PutRequest request;
    public CompletableFuture<Boolean> cf;
    public boolean resposta;

    public Put(PutRequest request, CompletableFuture<Boolean> cf) {
        this.request = request;
        this.cf = cf;
    }

    public Put(PutRequest request, CompletableFuture<Boolean> cf, boolean resposta) {
        this.request = request;
        this.cf = cf;
        this.resposta = resposta;
    }

    public void setResposta(boolean r){
        resposta = r;
    }

}
