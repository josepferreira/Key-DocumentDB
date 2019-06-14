package messages;

public class RestartRequest {

    public String endereco;
    public String id;

    public RestartRequest(String id, String endereco) {
        this.id = id;
        this.endereco = endereco;
    }
}
