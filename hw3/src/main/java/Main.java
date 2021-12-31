

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
    static int len = 8298;
    public static void main(String[] args) throws IOException {
        // adjMatrix is the adjacent matrix of all nodes
        int[][] adjMatrix = readFile();

        // set is all the non-duplicated node that have out-degree
        HashSet<Integer> set = new HashSet<>();
        for (int i = 0; i < len; i++) {
//            int temp = 0;
//            for (int j = 0; j < len; j++) {
//                temp = temp | adjMatrix[i][j];
//            }
//            if (temp == 1) {
                set.add(i);
//                System.out.println(i);
//            }
        }

        HashMap<Integer, HyperLogLog> map = new HashMap<>();
        for(int index: set){
            // initialize itself need a HyperLog counter
            map.put(index, new HyperLogLog(14)); // here b set to 10 (NEED CHANGE)
            map.get(index).initializeNode(adjMatrix[index]);
        }

        HyperBall hyperBall = new HyperBall(16384, map);// 2^14

        int[] centralities = hyperBall.doHyperBall(adjMatrix, set);
        for (int i = 0; i < centralities.length; i++) {
            if (centralities[i] != 0) {
                System.out.println(i + ":" + centralities[i]);
            }
        }



//        System.out.println("____________________________________");
//        System.out.println("Kafka inputs");
//        System.out.println("____________________________________");
//
//
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(10);
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.println("" + record.value());
//                String input = record.value();
//                int from = Integer.valueOf(input.trim().split("\\s")[0].trim());
//                int to = Integer.valueOf(input.trim().split("\\s")[1].trim());
//                adjMatrix[from][to] = 1;

//        for(int index: set){
//            // initialize itself need a HyperLog counter
//            map.put(index, new HyperLogLog(14)); // here b set to 10 (NEED CHANGE)
//            map.get(index).initializeNode(adjMatrix[index]);
//        }

//                centralities = hyperBall.doHyperBall(adjMatrix, set);
//                for (int i = 0; i < centralities.length; i++) {
//                    if (centralities[i] != 0) {
//                        System.out.println(i + ":" + centralities[i]);
//                    }
//                }
//            }
//        }
//


    }

    //read file and return an adjMatrix
    public static int[][] readFile() throws IOException {

        //read file as stream input
        Path path = Paths.get("./hw3/dataset/Wiki-Vote.txt");
        Stream<String> stream = Files.lines(path);
        List<String> list = stream.collect(Collectors.toList());

//        File file = new File("./hw3/dataset/Wiki-Vote.txt");
//        Scanner sc = new Scanner(file);
//        sc.nextLine();
//        sc.nextLine();
//        sc.nextLine();
//        sc.nextLine();


//        max is 8297
//        int max = 0;
//        while(sc.hasNext()){
//            max = Math.max(max, sc.nextInt());
//        }
//        System.out.println(max);

        //generate adj matrix
        int[][] adjMatrix = new int[len][len];
//        while (sc.hasNextLine() && sc.hasNextInt()) {
//            int from = sc.nextInt();
//            int to = sc.nextInt();
//            adjMatrix[from][to] = 1;
//        }


        for(int i = 4; i < list.size(); i ++){
            int from = Integer.valueOf(list.get(i).trim().split("\\s")[0].trim());
            int to = Integer.valueOf(list.get(i).trim().split("\\s")[1].trim());
            adjMatrix[from][to] = 1;
        }

//        for(int i : adjMatrix[30]){
//            if(i == 1){
//                System.out.println(i);
//            }
//        }

        return adjMatrix;
    }




//    private static final String TOPIC = "test";
//    private static final String BROKER_LIST = "localhost:9092";
//    private static KafkaConsumer<String,String> consumer = null;
//
//    static {
//        Properties configs = initConfig();
//        consumer = new KafkaConsumer<String, String>(configs);
//        List<String> topics = new ArrayList<>();
//        topics.add("test");
//        consumer.subscribe(topics);
//    }
//
//    private static Properties initConfig(){
//        Properties properties = new Properties();
//        properties.put("bootstrap.servers",BROKER_LIST);
//        properties.put("group.id","0");
//        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("enable.auto.commit", "true");
//        properties.setProperty("auto.offset.reset", "earliest");
//        return properties;
//    }

}
