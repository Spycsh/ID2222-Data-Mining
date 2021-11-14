import java.util.*;

public class LSH {
    int b;  // band number
    int r;  // how many rows for one band
    int K; // how many buckets for one band

    // finds candidate pairs of signatures agreeing on
    // at least fraction t of their components
    // return a list of doc index that need to be compared
    public HashSet<Integer> getCandidates(List<int[]> signatures, int b, int K) {
        int N = signatures.size();          // the length of signatures
        int M = signatures.get(0).length;   // the length of a signature
        this.b = b;
        this.r = M / b;
        this.K = K;

        // for each band, store an array of all partial signatures
        int[][] bandInput = new int[b][K];

//        List<String> signatureStrList = new ArrayList<>();
//        for(int i=0; i<N; i++){
//            int[] curSignature = signatures.get(i);
//            StringBuilder sb = new StringBuilder();
//            for(int j=0; j<curSignature.length; j++){
//                sb.append(curSignature[j]);
//            }
//            signatureStrList.add(sb.toString());
//        }

        HashSet<Integer> res = new HashSet<>();

        // for all bands
        for (int i = 0; i < b; i++) { // for the i-th band
            // get the region index
            int startIdx = i * r;
            int endIdx = (i * r + r) > M ? M : (i + 1) * r;
            HashMap<Integer, List<Integer>> buckets = new HashMap<>();   // mapping from the bucket index to list of indexes of signatures
            for (int j = 0; j < N; j++) {
                // for the j-th signature, select out the area

                StringBuilder currentStr = new StringBuilder();
                for (int idx = startIdx; idx < endIdx - 1; idx++) {
                    currentStr.append(signatures.get(j)[idx]);
                }
                System.out.println(currentStr);

                // do not use string builder to calculate hash code
                // because it do not overwrite hashcode function
                // and two string builder with the same value have different hashcode
                int bucketIdx = currentStr.toString().hashCode() % K;

                List<Integer> signatureIndexes = buckets.getOrDefault(bucketIdx, new ArrayList<>());
                signatureIndexes.add(j);
                buckets.put(bucketIdx, signatureIndexes);
            }
            for (int k = 0; k < K; k++) { // for k-th bucketIdx
                if (buckets.get(k) != null && buckets.get(k).size() >= 2) {
                    for (int signatureIdx : buckets.get(k)) {
                        res.add(signatureIdx);
                    }
                }
            }
        }
        return res;

    }

}
