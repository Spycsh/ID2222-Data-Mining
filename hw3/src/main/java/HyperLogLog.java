import java.util.Arrays;
import java.util.Objects;

public class HyperLogLog {
    int b;
    int p;
    int[] counter;
    HyperLogLog(int b){
        this.b = b;
        this.p = (int) Math.pow(2, b);
        this.counter = new int[p];
    }

    // for a node that has a hyperloglog counter
    // it should be initialized with all accessed neighbor items
    public void initializeNode(int[] adjacentItems){
        for(int i=0; i<adjacentItems.length; i++){
            if(adjacentItems[i] ==1){
                add(String.valueOf(i));
            }
        }
    }

    public void add(String item){
        int hashedItem = hash(item);

        int i = getPrefix(hashedItem);
        counter[i] = Math.max(counter[i], getLeadingZerosNum(hashedItem));
    }

    public int size(){
        double sum = 0;
        for(int j=0; j<p-1; j++){
//            if(counter[j] != 0) System.out.println(counter[j]);
            sum += Math.pow(2, -counter[j]);
        }
        double Z = Math.pow(sum, -1);
        double a_p = 0.7213/(1+ 1.079/p);   // shown in Fig. 3 in HyperLogLog paper
        double E = a_p * Math.pow(p, 2) * Z;
        return (int) E;
    }

    private int hash(String item) {
        return item.hashCode();
    }

    // get the first b bit as the prefix (index of the counter)
    private int getPrefix(int hashedItem){
        int bitCount = getBitCount(hashedItem);
        return hashedItem >> (bitCount - b);
    }

    // get the leading zeros number of the suffix
    private int getLeadingZerosNum(int hashedItem){
        int bitCount = getBitCount(hashedItem);

        int mask = (int)Math.pow(2, bitCount - b) - 1;  // get the mask 00000...01111, number of 0 is b
        int lowerBits = mask & hashedItem;  // lowerBits should be a sequence with (bitCount - b) bits
        int lowerBitsCount = getBitCount(lowerBits);    // 1..01, the leading zeros are omitted here

        int leadingZerosNum = bitCount - b - lowerBitsCount;    // remain length minus lowerBits count
        return leadingZerosNum;
    }

    private int getBitCount(int num){
        int cnt = 0;
        for(; num!=0; num >>= 1){
            cnt++;
        }
        return cnt;
    }

    public int[] getCounter() {
        return counter;
    }

    public void setCounter(int[] counter) {
        this.counter = counter;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HyperLogLog that = (HyperLogLog) o;
        return b == that.b && p == that.p && Arrays.equals(counter, that.counter);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(b, p);
        result = 31 * result + Arrays.hashCode(counter);
        return result;
    }


}
