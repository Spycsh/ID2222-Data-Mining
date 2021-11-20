import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;


import javax.xml.crypto.Data;
import java.io.File;
import java.util.*;

public class MinHashing {

    int M = 0;  // how many different shingles in total
    int N = 0;  // how many different sets (documents)
    int K = 0;  // how many different hash functions

    int RAND = 200;

    // builds a minHash signature (in the form of a vector or a set)
    // of a given length n from a given set of integers (a set of hashed shingles).
    // the main idea is to convert a M * N shingle-document matrix to a
    // K * N signature matrix
    // the algorithm is to resolve the problem when shingle size M is too large
    // so we can use K hash function on the column vector and select the top one (permutation)
    // to obtain K lines of signatures which is far smaller than the original shingle size M
    public List<int[]> createMinHashSignatures(int _K, String shingleResultsDirPath) throws Exception {
        List<int[]> signatures = new ArrayList<>();

        K = _K;
        File shingleResultsDir = new File(shingleResultsDirPath);
        String[] docs = shingleResultsDir.list();

        N = docs.length;

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // calculate the total set size of the shingles and get the totalSet of all shingles
        DataSet<String> totalSet = env.readTextFile(shingleResultsDirPath)
                .flatMap(new getTotalSet())
                .distinct();
        long totalShingleSize = totalSet.count();
        M = (int) totalShingleSize;

        // coefficient matrix of the hash functions
        int[][] coefficients = new int[K][2];
        Random rand = new Random();
        // calculate the coefficient of the hash function
        // (ax + b) % c
        for(int i = 0; i<K; i++){
            coefficients[i][0] = rand.nextInt(RAND);    // a
            coefficients[i][1] = rand.nextInt(RAND);    // b
//            System.out.println(coefficients[i][0]+":"+coefficients[i][1]);
        }
        int c = M+1;
//         c should be the a prime number that is larger than M
//         https://www.zhihu.com/question/20806796/answer/2232672533
        while(!isPrime(c)){
            c++;
        }
        System.out.println("c is: "+c);

        HashMap<String, Integer> shingleMap = new HashMap<>();
        int idx = 0;
        for(String str : totalSet.collect()){
            shingleMap.put(str, idx++);
        }

        List<String> totalSetList = totalSet.collect();
        for(String doc: docs){
            int[] vector = new int[M];
            List<String> hashcodes = env.readTextFile(shingleResultsDirPath + "\\" + doc).setParallelism(1).collect();

            for(int i=0; i<totalSetList.size(); i++){
                if(hashcodes.contains(totalSetList.get(i))){
                    vector[i] = 1;
                }
            }

            int[] resVector = new int[K];
            for(int i=0; i<K; i++){
                int a = coefficients[i][0];
                int b = coefficients[i][1];
                int min = Integer.MAX_VALUE;
                for(int t=0; t<vector.length; t++){
                    if(vector[t] == 1){
                        int index = (t * a + b) % c;
                        min = Math.min(index, min);

                    }
                }
                resVector[i] = min;
            }
            System.out.println("doc "+ doc +" has min-hashing " + Arrays.toString(resVector));

            signatures.add(resVector);
        }

        return signatures;
    }

    public static class getTotalSet implements FlatMapFunction<String, String>{

        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            String[] hashcodes = s.split(System.lineSeparator());
            for (String hashcode : hashcodes) {
                collector.collect(hashcode);
            }
        }
    }

    public boolean isPrime(int n){
        // Corner case
        if (n <= 1)
            return false;

        // Check from 2 to n-1
        for (int i = 2; i < Math.sqrt(n); i++)
            if (n % i == 0)
                return false;

        return true;
    }

}
