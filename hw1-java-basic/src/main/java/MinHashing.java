import java.math.BigInteger;
import java.util.*;

//A class MinHashing that builds a minHash signature
// (in the form of a vector or a set) of a given length n
// from a given set of integers (a set of hashed shingles).
public class MinHashing {
    //row: shinglings
    //col: datasets + hash functions
    int[][] matrix;

    //the matrix of signatures
    //row: hash functions
    //col: data sets
    int[][] signatureMatrix;

    //number of hash functions
    int k = 100;

    //hash function: ( a * x + b ) mod c
    //a and b of all the hash functions are stored here
    ArrayList<Integer[]> hashFunctions = new ArrayList<>();

    //the prime number in all the hash function
    int c = 44497;


    Integer[] allShinglings;


    //all names of files
    String[] sets;

    //vectors of sets
    LinkedHashMap<String, ArrayList<Integer>> vectors = new LinkedHashMap<>();


    public MinHashing(HashSet<Integer> union, Shingling[] shinglings, String[] sets){
        this.sets = sets;

        //row number is shinglings number
        //column number should be sets size + hash function size
        matrix = new int[union.size()][sets.length + k];

        allShinglings = union.toArray(new Integer[union.size()]);

        //fill in the matrix
        for(int i = 0; i < matrix.length; i ++){

            //first part of the matrix
            //row: shinglings
            //col: datasets
            //element: if the current shingling exists in the current dataset
            for(int j = 0; j < sets.length; j ++) {
                if (shinglings[j].getHashedShingling().contains(allShinglings[i])) {
                    matrix[i][j] = 1;
                } else {
                    matrix[i][j] = 0;
                }
            }

            //second part of the matrix
            //row: shinglings
            //col: hash functions (a and b are randomly generated)
            //element: apply row index to the hash function
            for(int j = sets.length; j < matrix[0].length; j ++){
                int a = (int)(Math.random() * 100) + 1;
                int b = (int)(Math.random() * 10) + 1;
                hashFunctions.add(new Integer[]{a,b});

                int temp = ((a * i) + b) % c;
                matrix[i][j] = temp;
            }

        }

//        System.out.println("________________matrix ________________");
//        printMatrix(matrix);
    }

    //generate the signature matrix
    public void generateSignatureMatrix(){
        //row number is hash function number
        //column number is dataset number
        signatureMatrix = new int[k][sets.length];
        for(int i = 0; i < signatureMatrix.length; i ++){
            Arrays.fill(signatureMatrix[i], Integer.MAX_VALUE);//fill in the max value initially
        }


        for(int i = 0; i < matrix.length; i ++){
            ArrayList<Integer> ones = new ArrayList<>();

            for(int j = 0; j < sets.length; j ++){
                if(matrix[i][j] == 1){
                    ones.add(j);//remember the columns that is 1 at the current row

                }
            }
            //System.out.println(ones.toString());


                //fill in the signature matrix
                for(int col : ones){//column
                    for(int row = 0; row < k; row ++){//row
                        //only rewrite if the current value is smaller
                        signatureMatrix[row][col] = Math.min(signatureMatrix[row][col], matrix[i][row + sets.length]);

                    }
                }

        }
        System.out.println("---------------signature matrix--------------");
        printMatrix(signatureMatrix);
    }

    //generate vectors for each dataset
    //the columns of the signature matrix
    public LinkedHashMap<String, ArrayList<Integer>> generateVectors(){
        for(int i = 0; i < signatureMatrix[0].length; i ++){//column
            ArrayList<Integer> temp = new ArrayList<>();
            for(int j = 0; j < signatureMatrix.length; j ++) {//row
                temp.add(signatureMatrix[j][i]);
            }
            vectors.put(sets[i], temp);
            System.out.println(temp.toString());
        }

        return vectors;
    }

    //print the matrix
    public void printMatrix(int[][] matrix){

        for(int i = 0; i < matrix.length; i ++){
            String str = "";
            boolean heading = true;
            for(int j : matrix[i]){
                if(heading){
                    str = str + j;
                    heading = false;
                }else{
                    str = str + ", " +j;

                }

            }
            System.out.println(str);
        }
    }

}
