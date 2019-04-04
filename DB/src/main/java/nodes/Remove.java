package nodes;

import messages.RemoveRequest;

import java.util.concurrent.CompletableFuture;

public class Remove {
    RemoveRequest request;
    public CompletableFuture<Boolean> cf;

    public Remove(RemoveRequest request, CompletableFuture<Boolean> cf) {
        this.request = request;
        this.cf = cf;
    }

}
