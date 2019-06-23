package Configuration;
import com.google.common.primitives.Longs;

import java.util.Arrays;
import java.util.Objects;


public class KeysUniverse implements Comparable{
    public byte[] min;
    public byte[] max;

    public KeysUniverse(byte[] min, byte[] max) {
        this.min = min;
        this.max = max;
    }

    public KeysUniverse(Object min, Object max) {
        this.min = Config.encode(min);
        this.max = Config.encode(max);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        KeysUniverse that = (KeysUniverse) o;

        if(Arrays.equals(min,that.min) && Arrays.equals(max,that.max)) return true;

        return (Config.compareArray(min,that.min) >= 0) && (Config.compareArray(max,that.max) < 0) && (Config.compareArray(min,that.max) <= 0); //basta que esteja contido no intervalo,
        // para ser considerado igual. Importante para a procura no hashmap
    }

    @Override
    public int hashCode() {
        return Objects.hash(min);
    }

    @Override
    public String toString() {
        return "KeysUniverse{" +
                "min=" + Config.decode(min) +
                ", max=" + Config.decode(max) +
                '}';
    }

    public String getGrupo(){
        try {
            long minA = Longs.fromByteArray(min);

            long maxA = Longs.fromByteArray(max);
            return minA + "," + maxA;
        }
        catch(Exception e){
        }
        String minA = new String(min);

        String maxA = new String(max);
        return minA + "," + maxA;

    }



    @Override
    public int compareTo(Object o) {
        KeysUniverse ku = (KeysUniverse) o;

//        if(this.min == this.max && this.min == ku.min)
        if(Config.compareArray(this.min,this.max) == 0 && Config.compareArray(this.min,ku.min) == 0)
            return 0;

//        if(ku.min >= this.max && ku.max >= this.max)
        if(Config.compareArray(ku.min,this.max) >= 0 && Config.compareArray(ku.max,this.max) >= 0)
            return -1;

//        if(ku.max <= this.min && ku.min <= this.min)
        if(Config.compareArray(ku.max,this.min) <= 0 && Config.compareArray(ku.min,this.min) <= 0)
            return 1;

        return 0;

        //return Long.compare(this.max,ku.max);
    }
}
