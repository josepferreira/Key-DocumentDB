package nodes;

import com.google.common.primitives.Longs;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class Config {
    public static int nSlaves = 3;
    public static int nConjuntos = 9;

    public static long periodoTempo = 15; //periodo de tempo em segundos
    public static TimeUnit unidade = TimeUnit.SECONDS;

    //Percentagens Utilizacao
    public static double cpuMax = 0.7;
    public static float memMin = 2;

    public static float valorMin = 0.6f;
    public static float valorMax = 4;

    public static int portaInicial = 12340;
    public static String hostAtomix = "localhost";
    public static String hostSpread = "localhost";

    public static int fatorReplicacao = 1;


    public static int compareArray(byte[] a, byte[] b){

        Object oA = decode(a);
        Object oB = decode(b);

        if(oA instanceof Long && oB instanceof Long){
            Long lA = (Long)oA;
            Long lB = (Long)oB;
            return Longs.compare(lA,lB);
        }

        String lA = (String)oA;
        String lB = (String)oB;
        return lA.compareTo(lB);

    }

    public static byte[] encode(Object key){
        if(key instanceof String){
            return ((String)key).getBytes();
        }
        else {
            if(key instanceof Integer){
                int aux = (Integer) key;
                key = new Long(aux);
            }

            if(key instanceof Long ){
                Long a = (Long)key;
                return Longs.toByteArray(a);
            }
        }
        return null;
    }

    public static Object decode(byte[] key){
        try {
            long keyA = Longs.fromByteArray(key);

            return keyA;
        }
        catch(Exception e){
            System.out.println("N era long");
        }
        String keyA = new String(key);

        return keyA;
    }

    public static void main(String[] args){
        long aux = 400;
        long aux2 = 300;


        int res = compareArray(Longs.toByteArray(aux),Longs.toByteArray(aux2));

        System.out.println("R: " + res);
        System.out.println("RS: " + Longs.compare(aux,aux2));
    }

}
