import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.ArrayList;
import java.util.Objects;

// constructs k–shingles of a given length k (e.g., 10) from a given document,
// computes a hash value for each unique shingle,
// and represents the document in the form of an ordered set of its hashed k-shingles.
public class Shingling {

    public ArrayList<DataSet<Integer>> createShingles(String dataDirPath, int N, String sinkDir) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        File dataDir = new File(dataDirPath);

        ArrayList<DataSet<Integer>> shingles = new ArrayList<>();
        int fileIdx = 0;
        for(String filePath: Objects.requireNonNull(dataDir.list())){
            DataSet<String> fileContent = env.readTextFile(dataDirPath + "\\" + filePath);
            // a try to reduce all lines into one line may not be effective
            // because flink does not guarantee sequential parallel processing of lines
            // without providing time or extra information
            DataSet<Integer> resultSet = fileContent.flatMap(new MyFlatMapper(N)).distinct();
            System.out.println(filePath+" shingles: "+resultSet.collect());

            // write to results folder, set parallelism to be 1 to keep all content in one file
            resultSet.writeAsText(sinkDir + "\\shingle" + fileIdx + ".txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            fileIdx++;
            shingles.add(resultSet); // distinct to avoid the same hashcode of shingles
        }
        return shingles;
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Integer> {
        private int N;

        MyFlatMapper(int N){
            this.N = N;
        }

        @Override
        // collect the shingle for every line of a file separately
        public void flatMap(String s, Collector<Integer> collector) throws Exception {
            int N = this.N;

            int L = s.length();
            int i = 0;

            if(L - N < 0){
                // line length smaller than shingle size
                // just put this line as a shingle
                collector.collect(s.substring(0).hashCode());
            }

            while(i <= L - N){
                collector.collect(s.substring(i, i + N).hashCode());
                i++;
            }
        }
    }
}
