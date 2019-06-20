package messages;

import nodes.KeysUniverse;
import nodes.ParEscritaLeitura;

import java.util.TreeMap;

public class InfoMonitorizacao {

    public float memoria;
    public double cpu;
    public TreeMap<KeysUniverse, ParEscritaLeitura> operacoes;

    public InfoMonitorizacao(float memoria, double cpu, TreeMap<KeysUniverse, ParEscritaLeitura> operacoes) {
        this.memoria = memoria;
        this.cpu = cpu;
        this.operacoes = operacoes;
    }

}
