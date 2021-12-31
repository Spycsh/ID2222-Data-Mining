import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

//A class MinHashing that builds a minHash signature
// (in the form of a vector or a set) of a given length n
// from a given set of integers (a set of hashed shingles).
public class MinHashingTest {
    int[][] matrix;
    int[][] signatureMatrix;
    int k = 2;

    Integer[] allShinglings;
    ArrayList<Integer[]> hashFunctions = new ArrayList<>();
    String[] sets;

    public MinHashingTest(){
        HashSet<Integer> union = new HashSet<>();
        union.add(0);
        union.add(1);
        union.add(2);
        union.add(3);
        union.add(4);

        this.sets = new String[]{"S1" ,"S2", "S3", "S4"};

        //row number is shinglings number
        //column number should be sets size + hash function size
        matrix = new int[union.size()][sets.length + k];

        allShinglings = union.toArray(new Integer[union.size()]);

        matrix[0] = new int[]{1, 0, 0, 1, (0 + 1) % 5, ((3 * 0) + 1) % 5};
        matrix[1] = new int[]{0, 0, 1, 0, (1 + 1) % 5, ((3 * 1) + 1) % 5};
        matrix[2] = new int[]{0, 1, 0, 1, (2 + 1) % 5, ((3 * 2) + 1) % 5};
        matrix[3] = new int[]{1, 0, 1, 1, (3 + 1) % 5, ((3 * 3) + 1) % 5};
        matrix[4] = new int[]{0, 0, 1, 0, (4 + 1) % 5, ((3 * 4) + 1) % 5};


        printMatrix(matrix);



    }

    public void generateSiguratureMatrix(){
        //row number is hash function number
        //column number is set number
        signatureMatrix = new int[k][sets.length];
        for(int i = 0; i < signatureMatrix.length; i ++){
            Arrays.fill(signatureMatrix[i], Integer.MAX_VALUE);
        }


        for(int i = 0; i < matrix.length; i ++){
            ArrayList<Integer> ones = new ArrayList<>();

            for(int j = 0; j < sets.length; j ++){
                if(matrix[i][j] == 1){
                    ones.add(j);//which column at current row is 1
                }
            }

            //fill in the signature matrix
            for(int col : ones){//column
                for(int row = 0; row < k; row ++){//row
                    signatureMatrix[row][col] = Math.min(signatureMatrix[row][col], matrix[i][row + sets.length]);

                }
            }
            System.out.println("_______________________");
            printMatrix(signatureMatrix);
        }

    }

    public void computeSim(int i, int j){
        double intersection = 0.0;
        double sim = 0.0;
        for(int r = 0; r < signatureMatrix.length; r ++){//row
            if(signatureMatrix[r][i] == signatureMatrix[r][j]){
                intersection = intersection + 1.0;
            }
        }

//        System.out.println("v1 " + v1.size());
//        System.out.println("v2 " + v2.size());
//        System.out.println("intersection " + intersection);

        sim = intersection / (double) signatureMatrix.length;
        System.out.println(sim);
    }

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
