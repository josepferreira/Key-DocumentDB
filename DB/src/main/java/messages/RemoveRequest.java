package messages;

public class RemoveRequest {
    public long key; //para já só uma key, depois pode ter várias
    public String id;

    //depois vai ter seleções e projeções


    public RemoveRequest(String id, long key) {
        this.id = id; this.key = key;
    }

    @Override
    public String toString() {
        return "GetRequest{" +
                "key=" + key +
                ", id='" + id + '\'' +
                '}';
    }
}
