package messages;

public class GetRequest {
    public long key; //para já só uma key, depois pode ter várias

    //depois vai ter seleções e projeções


    public GetRequest(long key) {
        this.key = key;
    }
}
