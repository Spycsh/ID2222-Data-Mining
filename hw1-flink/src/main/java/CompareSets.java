import org.apache.flink.api.java.DataSet;

public class CompareSets{

    public double computeJaccardSimilarity(DataSet<Integer> ds1, DataSet<Integer> ds2) throws Exception {
        return (double) ds1.join(ds2).where("*").equalTo("*").count() / (double) ds1.union(ds2).distinct().count();
    }
}
