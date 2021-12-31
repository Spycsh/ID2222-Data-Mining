import org.apache.flink.api.java.DataSet;

import java.util.*;

public class Apriori {
    //transactions in the form of <items, number>
    HashMap<List<Long>, Integer> transactions = new HashMap<>();

    HashSet<Long> c1 = new HashSet<>();
    HashSet<Long> l1 = new HashSet<>();

    HashSet<HashSet<Long>> c2 = new HashSet<>();
    HashSet<HashSet<Long>> l2 = new HashSet<>();

    HashSet<HashSet<Long>> c3 = new HashSet<>();
    HashSet<HashSet<Long>> l3 = new HashSet<>();

    // frequent itemset number threshold
    int s;

    // confidence threshold
    double c;

    public Apriori(int s){
        this.s = s;
    }

    public HashSet<HashSet<Long>> aprioriAlgorithm(List<String[]> transactions){
        //initialize transactions
        for(String[] trans: transactions){
            ArrayList<Long> items = new ArrayList<>();
            for(String item : trans){
                items.add(Long.valueOf(item));
            }
            if(this.transactions.containsKey(items)){
                int num = this.transactions.get(items) + 1;
                this.transactions.put(items, num);
            }else{
                this.transactions.put(items, 1);
            }

        }

        //get all distinct single items
        getSingleItems();

        //first iteration
        //calculate support for each single items
        for(Long i :  c1){
            HashSet<Long> temp = new HashSet<>();
            temp.add(i);
            if(support(temp) >= s) { // if the support is smaller than the threshold, then remove this item
                 l1.add(i);
            }
        }
        System.out.println("l1 size : " + l1.size());
        generateDataForSecondIteration();

        //second iteration
        //calculate support for each item sets with 2 elements
        for(HashSet<Long> i : c2){
            //System.out.println(i.toString());
            //System.out.println(i.toString());
            if(support(i) >= s){
                l2.add(i);

            }
        }
        System.out.println("l2 size : " + l2.size());

        generateDataForThirdIteration();

        //third iteration
        //calculate support for each item sets with 3 elements
        for(HashSet<Long> i : c3){
            if(support(i) >= s){
                l3.add(i);
                //System.out.println(l3.toString());
            }
        }
        System.out.println("l3 size : " + l3.size());

        return l3;
    }

    //get all distinct single items
    public HashSet<Long> getSingleItems(){
        HashSet<Long> items = new HashSet<>();

        for(List<Long> trans : transactions.keySet()){
            for(Long item : trans) {
                items.add(item);
            }
        }
        c1 = items;
        System.out.println("c1 size : " + c1.size());
        return items;
    }

    //compute support for one item set eg. {A} or {A, B}
    public int support(HashSet<Long> item){
        int support = 0;
        for(List<Long> items : transactions.keySet()){//for one transaction

            HashMap<Long, Boolean> itemExist = new HashMap<>();
            for(Long i : item){//for each item in the item set (from the hash set parameter)
                itemExist.put(i, items.contains(i));
            }

            boolean allExist = true;
            for(Boolean e : itemExist.values()){
                allExist = allExist && e;
            }

            //if all the items in the set exist in this transaction
            if(allExist){
                support = support + transactions.get(items);
            }

        }
        return support;
    }

    //generate data for second iteration
    public void generateDataForSecondIteration(){
        for(Long i : l1){
            for(Long j : l1){
                if(i != j){
                    HashSet<Long> temp = new HashSet<>();
                    temp.add(i);
                    temp.add(j);
                    c2.add(temp);
                }
            }
        }
        System.out.println("c2 size : " + c2.size());

    }

    //generate data for third iteration
    public void generateDataForThirdIteration(){
        for(HashSet<Long> i : l2){
            for(Long j : l1){
                if(!i.contains(j)){
                    HashSet<Long> temp = new HashSet<>(i);
                    temp.add(j);
                    c3.add(temp);
                }
            }
        }
        System.out.println("c3 size : " + c3.size());

    }

    public void findRules(double confidence){
        // s is the support threshold
        // c is the confidence threshold of the rules
        this.c = confidence;

        // find associations rules for l2
        for(HashSet<Long> e:l2){
            Long[] tuple2 = e.toArray(new Long[e.size()]);
            if(calculateConfidence(tuple2[0], tuple2[1]) > c){
                // tuple2[0] -> tuple2[1]
                System.out.println(tuple2[0] + "->" + tuple2[1] + " is an association rule");
            }
            if(calculateConfidence(tuple2[1], tuple2[0]) > c){
                // tuple2[0] -> tuple2[1]
                System.out.println(tuple2[1] + "->" + tuple2[0] + " is an association rule");
            }
        }

        // find association rules for l3
        for(HashSet<Long> e:l3){
            Long[] tuple3 = e.toArray(new Long[e.size()]);
            HashSet<Long> set12 =  new HashSet<Long>(){{
                add(tuple3[1]);
                add(tuple3[2]);
            }};

            HashSet<Long> set02 = new HashSet<Long>(){{
                add(tuple3[0]);
                add(tuple3[2]);
            }};
            HashSet<Long> set01 = new HashSet<Long>(){{
                add(tuple3[0]);
                add(tuple3[1]);
            }};

            if(calculateConfidence(tuple3[0], set12 )> c){
                System.out.println(tuple3[0] + "->" + set12.toString() + " is an association rule");
            }
            if(calculateConfidence(set12, tuple3[0]) > c){
                System.out.println(set12.toString() + "->" + tuple3[0] + " is an association rule");
            }
            if(calculateConfidence(tuple3[1], set02 )> c){
                System.out.println(tuple3[1] + "->" + set02.toString() + " is an association rule");
            }
            if(calculateConfidence(set02, tuple3[1]) > c){
                System.out.println(set02.toString() + "->" + tuple3[1] + " is an association rule");
            }
            if(calculateConfidence(tuple3[2], set01 )> c){
                System.out.println(tuple3[2] + "->" + set01.toString() + " is an association rule");
            }
            if(calculateConfidence(set01, tuple3[2]) > c){
                System.out.println(set01.toString() + "->" + tuple3[2] + " is an association rule");
            }
        }

    }


    public double calculateConfidence(Long num1, Long num2){
        return calculateConfidence(new HashSet<Long>(){{add(num1);}}, new HashSet<Long>(){{add(num2);}});
    }

    public double calculateConfidence(Long num1, HashSet<Long> set){
        return calculateConfidence(new HashSet<Long>(){{add(num1);}}, set);
    }

    public double calculateConfidence(HashSet<Long> set, Long num2){
        return calculateConfidence(set, new HashSet<Long>(){{add(num2);}});
    }

    public double calculateConfidence(HashSet<Long> set1, HashSet<Long> set2){
        return (double)support(union(set1, set2)) / (double) support(set1);
    }

    private HashSet<Long> union(HashSet<Long> set1, HashSet<Long> set2){
        HashSet<Long> resultSet = new HashSet<>();
        for(Long e: set1) resultSet.add(e);
        for(Long e: set2) resultSet.add(e);
        return resultSet;
    }

}
