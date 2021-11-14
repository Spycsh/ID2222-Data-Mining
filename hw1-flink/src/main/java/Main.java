import org.apache.flink.api.java.DataSet;
import scala.Int;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception{
        Shingling shingling = new Shingling();
        String inputPath = "C:\\Users\\Spycsh\\Desktop\\hw1-flink\\src\\main\\resources\\dataset";
        String resultPath = "C:\\Users\\Spycsh\\Desktop\\hw1-flink\\src\\main\\results";

        // shingle results will be written into outputPath
        ArrayList<DataSet<Integer>> shingles = shingling.createShingles(inputPath, 5, resultPath + "\\shingle_results");

        CompareSets compareSets = new CompareSets();

        for(int i=0; i<shingles.size(); i++){
            for(int j=i+1; j<shingles.size(); j++){
                double JaccardSimilarity = compareSets.computeJaccardSimilarity(shingles.get(i),shingles.get(j));
                System.out.println("Jaccard similarity between "+"shingle"+i+" and shingle"+j+":"+JaccardSimilarity);
            }
        }

        MinHashing minHashing = new MinHashing();
        List<int[]> minHashSignatures = minHashing.createMinHashSignatures(10, resultPath + "\\shingle_results");
        CompareSignatures compareSignatures = new CompareSignatures();
        for(int i=0; i<minHashSignatures.size(); i++){
            for(int j=i+1; j<minHashSignatures.size(); j++){
                double signatureSimilarity = compareSignatures.compareSignatures(minHashSignatures.get(i), minHashSignatures.get(j));
                System.out.println("signature similarity between "+"signature"+i+" and signature"+j+":"+signatureSimilarity);
            }
        }

        LSH lsh = new LSH();
        HashSet<Integer> signatureIndexes = lsh.getCandidates(minHashSignatures, 3, 1000);

        System.out.println(signatureIndexes);


    }
}
