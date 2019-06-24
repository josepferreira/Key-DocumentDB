package Configuration;

import com.google.common.primitives.Longs;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Config {
    public static int fatorReplicacao = 1;
    public static int nSlaves = (3 < (Config.fatorReplicacao+1) ? (Config.fatorReplicacao+1) : 3);
    public static int nConjuntos = 9;
    public static int chunk = 50;

    public static long periodoTempo = 45; //periodo de tempo em segundos
    public static TimeUnit unidade = TimeUnit.SECONDS;

    //Percentagens Utilizacao
    public static double cpuMax = 0.7;
    public static float memMin = 2;

    public static float valorMin = 0.6f;
    public static float valorMax = 4;

    public static int portaInicial = 20000;
    public static int rangePortas = 20000;
    public static String hostAtomix = "localhost";
    public static String hostSpread = "localhost";


    public static boolean eLong = true;

    public static int getPorta(){
        Random r = new Random();

        return r.nextInt(Config.rangePortas) + Config.portaInicial;
    }

    public static String[] daChave(){
        char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        String[] res = new String[alphabet.length];
        int i = 0;
        for(char c: alphabet){
            String a = String.valueOf(c);
            res[i++] = a;
        }
        return res;
    }

    public static ArrayList<byte[]> conjuntosChave(){
        System.out.println("Conjuntos de chave");
        if(eLong){
            ArrayList<byte[]> res = new ArrayList<>();
            for(int i = 0; i  < nConjuntos; i++){
                long atual = i * chunk;
                res.add(Longs.toByteArray(atual));
            }
            res.add(Longs.toByteArray(Long.MAX_VALUE));
            return res;
        }
        else{
            String last = String.valueOf('{');
            String[] aux = daChave();
            if(nConjuntos > aux.length){
                nConjuntos = aux.length;
            }


            int step = aux.length / nConjuntos;
            if(aux.length % nConjuntos != 0){
                step++;
            }
            int atual = 0;
            ArrayList<byte[]> res = new ArrayList<>();
            for(int i = 0; i < nConjuntos; i++){
                atual =  i * step;
                if(atual >= aux.length){
                    atual = aux.length-1;
                }
                res.add(aux[atual].getBytes());
            }
            res.add(last.getBytes());
            return res;
        }
    }


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
