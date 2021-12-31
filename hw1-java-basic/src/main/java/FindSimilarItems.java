import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

public class FindSimilarItems {

    public static void main(String[] args) throws UnsupportedEncodingException {

        //generate shinglings
        Shingling[] shinglings = new Shingling[5];
        String[] files = {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt"};

        for(int i = 0; i < 5; i ++){
            shinglings[i] = new Shingling();
            shinglings[i].readFile(files[i]);
            shinglings[i].generateShingling();
        }



        //compare using hash code of the strings
        for(int i = 0; i < 5; i ++){
            for(int j = i + 1; j < 5; j ++){
                double sim = computeJaccardSim(shinglings[i].getHashedShingling(), shinglings[j].getHashedShingling());
                System.out.println("Jaccard sim (" + i + ", " + j + ")" + " : " + sim);
            }
        }



        //compare using minHashing
        HashSet<Integer> unionShinglings = new HashSet<>();
        for(int i = 0; i < 5; i ++){
            for(int s : shinglings[i].getHashedShingling()){
                unionShinglings.add(s);
            }
        }
        MinHashing minHashing = new MinHashing(unionShinglings, shinglings, files);
        minHashing.generateSignatureMatrix();
        LinkedHashMap<String, ArrayList<Integer>> vectors = minHashing.generateVectors();



//        MinHashingTest test = new MinHashingTest();
//        test.generateSiguratureMatrix();
//        test.computeSim(2, 3);


        //compare the signatures using min hashing
        for(int i = 0; i < vectors.size(); i ++){
            for(int j = i + 1; j < vectors.size(); j ++){
                System.out.println("Jaccard Signature sim (" + i + ", " + j + ")" + computeJaccardSimUsingSign(vectors.get(files[i]), vectors.get(files[j])));
            }
        }

        //System.out.println(vectors.get(files[0]).toString());
        //System.out.println("Jaccard Signature sim (" + 0 + ", " + 1 + ")" + computeJaccardSimUsingSign(vectors.get(files[0]), vectors.get(files[1])));
    }

    public static double computeJaccardSim(HashSet<Integer> set1, HashSet<Integer> set2){
        HashSet<Integer> union = new HashSet<>();

        for(int i : set1){
            union.add(i);
        }

        HashSet<Integer> intersection = new HashSet<>();
        for(int i : set2){
            if(union.contains(i)){
                //if the element already exists
                intersection.add(i);
                //System.out.println("intersection " + i);
            }else{
                union.add(i);
            }
        }


        double sim = (double) intersection.size() / (double) union.size();
        //System.out.println("intersection " + intersection.size() );
        //System.out.println("union " + union.size() );
        //System.out.println("sim " + sim);
        return sim;
    }


    public static double computeJaccardSimUsingSign(ArrayList<Integer> v1, ArrayList<Integer> v2){
        double intersection = 0.0;
        double sim;
        for(int i = 0; i < v1.size(); i ++){
            if(v1.get(i) == v2.get(i)){
//                System.out.println(v1.get(i));
//                System.out.println(v2.get(i));
                intersection = intersection + 1.0;
            }
        }

//        System.out.println("v1 " + v1.size());
//        System.out.println("v2 " + v2.size());
//        System.out.println("intersection " + intersection);

        sim = intersection / (double) v1.size();
        return sim;
    }


}
