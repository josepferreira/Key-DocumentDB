package nodes;
import java.util.Objects;

public class KeysUniverse implements Comparable{
    public long min;
    public long max;

    public KeysUniverse(long min, long max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;
        KeysUniverse that = (KeysUniverse) o;

        return (min >= that.min) && (max < that.max) && (that.min <= that.max); //basta que esteja contido no intervalo,
        // para ser considerado igual. Importante para a procura no hashmap
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max);
    }

    @Override
    public String toString() {
        return "KeysUniverse{" +
                "min=" + min +
                ", max=" + max +
                '}';
    }


    @Override
    public int compareTo(Object o) {
        KeysUniverse ku = (KeysUniverse) o;

        if(ku.min >= this.max && ku.max >= this.max)
            return -1;

        if(ku.max <= this.min && ku.min <= this.min)
            return 1;

        return 0;

        //return Long.compare(this.max,ku.max);
    }
}
