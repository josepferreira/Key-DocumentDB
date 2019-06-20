package nodes;

import java.util.concurrent.TimeUnit;

public class Config {
    public static int nSlaves = 3;
    public static int nConjuntos = 9;

    public static long periodoTempo = 300; //periodo de tempo em segundos
    public static TimeUnit unidade = TimeUnit.SECONDS;

    //Percentagens Utilizacao
    public static double cpuMax = 0.7;
    public static float memMin = 2;

    public static float valorMin = 0.6f;
    public static float valorMax = 4;

}
