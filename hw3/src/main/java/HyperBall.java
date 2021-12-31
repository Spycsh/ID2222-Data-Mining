import java.util.*;

public class HyperBall {
    //p = 2 ^ b registers
    int p;
    HashMap<Integer, HyperLogLog> map;

    public HyperBall(int p, HashMap<Integer, HyperLogLog> map) {
        this.p = p;
        this.map = map;
    }

    //union of two HyperLogLog counters
    public void union(HyperLogLog c1, HyperLogLog c2) {

        int[] counter1 = c1.getCounter();
        int[] counter2 = c2.getCounter();
        int[] counterNew = new int[p];
        for (int i = 0; i < p; i++) {
            counterNew[i] = Math.max(counter1[i], counter2[i]);

        }
        c1.setCounter(counterNew);

    }

    //hyper ball algorithm
    public int[] doHyperBall(int[][] adjMatrix, HashSet<Integer> set) {
        int t = 1;

        // the final result
        int[] centrality = new int[adjMatrix.length];


        while (true) {
            boolean STABILIZED = true;

            for (int index : set) {
                //get the HyperLogLog instance of the current node
                HyperLogLog curCounter = map.get(index);
                //a: hashCode and size of the original counter
                int originalCounterHashCode = curCounter.hashCode();
                int originalCounterSize = curCounter.size();

                //loop over the neighbours
                for (int j = 0; j < adjMatrix.length; j++) {
                    if (adjMatrix[index][j] == 1) {
                        HyperLogLog neighborCounter = map.get(j);

                        // union: update the array c[-]
                        union(curCounter, neighborCounter);
                    }
                }

                // write <v,a> to disk? Why?

                // do something with a and c[v]
                // here we sum the harmonious average value for each iteration
                // to get the centrality
                // 1/t * (a - c[v])
                centrality[index] += 1 / t * (curCounter.size() - originalCounterSize);
                //System.out.println(curCounter.size() + ", " + originalCounterSize);

                // if original does not equal to the new counter
                // there must be still not stabilized
                if (curCounter.hashCode() != originalCounterHashCode) {
                    STABILIZED = false;
                }
            }

            //if it is stabilized, then jump out of the loop
            if (STABILIZED) {
                break;
            }

            t += 1;
        }
        System.out.println("t is " + t);
        return centrality;


    }
}
