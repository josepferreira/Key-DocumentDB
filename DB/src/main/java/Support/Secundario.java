package Support;

public class Secundario implements Comparable{
    public String id;
    public String endereco;
    public boolean ativo;

    public Secundario(String id, String endereco, boolean ativo) {
        this.id = id;
        this.endereco = endereco;
        this.ativo = ativo;
    }

    @Override
    public String toString() {
        return "Secundario{" +
                "id='" + id + '\'' +
                ", endereco='" + endereco + '\'' +
                ", ativo=" + ativo +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        Secundario sc = (Secundario)o;
        return id.compareTo(sc.id);
    }
}
