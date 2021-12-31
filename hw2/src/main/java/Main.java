import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception {
        long startTime=System.currentTimeMillis();
        String datasetPath = ".\\hw2\\dataset\\T10I4D100K.dat";
        List<String[]> transactions = readFile(datasetPath);
        long endTime1=System.currentTimeMillis();
        System.out.println("time of reading the file : " + (endTime1 - startTime) + " milliseconds");

        Apriori ap = new Apriori(1000);
        HashSet<HashSet<Long>> frequentItems = ap.aprioriAlgorithm(transactions);
        System.out.println(frequentItems.toString());

        long endTime2=System.currentTimeMillis();
        System.out.println("time of finding all frequent itemsets: " + (endTime2 - endTime1) + "milliseconds");

        ap.findRules(0.3);
        long endTime3=System.currentTimeMillis();
        System.out.println("time of finding the associated rules: " + (endTime3 - endTime2) + "milliseconds");


    }

    public static List<String[]> readFile(String filePath) throws Exception {
        //read file
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> fileContent = env.readTextFile(filePath);

        //get the items for each transaction
        DataSet<String[]> transactions = fileContent.map(new MapFunction<String, String[]>() {
            @Override
            public String[] map(String s) throws Exception {
                String[] split = s.trim().split(" ");
                return split;
            }
        });
        return transactions.collect();
    }

//    public static class Splitter implements FlatMapFunction<String, Tuple2<Long, Integer>> {
//        @Override
//        public void flatMap(String sentence, Collector<Tuple2<Long, Integer>> out) throws Exception {
//            for (String word: sentence.split(" ")) {
//                out.collect(new Tuple2<Long, Integer>(Long.valueOf(word), 1));
//            }
//        }
//    }

}
