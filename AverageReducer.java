import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

// vv MaxTemperatureReducer2
public class AverageReducer extends MapReduceBase
        implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterator<DoubleWritable> values,
                       OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException {

        double max_temp = 0;
        int count = 0;
        while (values.hasNext()) {
            max_temp += values.next().get();
            count+=1;
        }
        double avg = max_temp/count;
        output.collect(key, new DoubleWritable(avg));
    }
}