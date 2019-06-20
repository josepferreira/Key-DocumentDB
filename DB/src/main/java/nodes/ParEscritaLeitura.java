package nodes;

public class ParEscritaLeitura {

    public int escritas;
    public int leituras;

    public ParEscritaLeitura(int escritas, int leituras) {
        this.escritas = escritas;
        this.leituras = leituras;
    }

    @Override
    public String toString() {
        return "ParEscritaLeitura{" +
                "escritas=" + escritas +
                ", leituras=" + leituras +
                '}';
    }
}

