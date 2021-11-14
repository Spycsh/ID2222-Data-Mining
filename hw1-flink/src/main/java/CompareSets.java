import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.File;
import java.util.List;
import java.util.Objects;

public class CompareSets{

    public double computeJaccardSimilarity(DataSet<Integer> ds1, DataSet<Integer> ds2) throws Exception {
        return (double) ds1.join(ds2).where("*").equalTo("*").count() / (double) ds1.union(ds2).distinct().count();
    }
}
