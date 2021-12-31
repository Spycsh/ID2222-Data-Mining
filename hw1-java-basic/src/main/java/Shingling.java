import java.io.*;
import java.util.HashMap;
import java.util.HashSet;

//A class Shingling that constructs k–shingles of a given length k (e.g., 10) from a given document,
// computes a hash value for each unique shingle, and represents the document in the form of an ordered
// set of its hashed k-shingles.
public class Shingling {
    //k value
    int k = 10;

    //string and the hashed value(integer)
    HashMap<String, Integer> map;

    //integer(hashed value) of all the strings
    HashSet<Integer> hashedShingling;

    //content of the file
    String content;

    public Shingling(){
        map = new HashMap<>();
    }

    //generate shingling
    public HashMap<String, Integer> generateShingling(){
        for(int i = 0; i < content.length() - k - 1; i ++){
            String tempS = content.substring(i, i + k);
            map.put(tempS, tempS.hashCode());
        }
        hashedShingling = new HashSet<Integer>(map.values());
        return map;
    }

    //read the file locally
    public void readFile(String fileName) throws UnsupportedEncodingException {
        fileName = fileName;

        File file = new File("./FindSimilarItems/dataset/" + fileName);
        Long fileLength = file.length();
        byte[] fileContent = new byte[fileLength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(fileContent);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        content = new String(fileContent, "UTF-8");
        //System.out.println(content);
        content = content.trim().replace("\n", " ").replace("\\s+", " ");
        //System.out.println(content);


    }

    public HashSet<Integer> getHashedShingling() {
        return hashedShingling;
    }
}
