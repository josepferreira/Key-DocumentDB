package messages;

public class StartRequest {

    public String endereco;
    public String id;

    public StartRequest(String id, String endereco) {
        this.id = id;
        this.endereco = endereco;
    }
}
