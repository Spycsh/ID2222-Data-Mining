
public class CompareSignatures {
    public double compareSignatures(int[] signature1, int[] signature2) throws Exception {
        int sim = 0;
        int L = signature1.length;
        for(int i=0; i<L; i++){
            if(signature2[i] == signature1[i]){
                sim++;
            }
        }
        return (double) sim / (double) L;
    }
}
