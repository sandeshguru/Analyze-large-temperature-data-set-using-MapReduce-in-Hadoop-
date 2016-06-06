import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class AverageMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable>
{

    public static final int MISSING = -9999;
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException
    {
        String line = value.toString();
        String[] path = line.split(",");
        String year = path[0].substring(0,4);
        double temperature;

        double Tmaxtemp = Double.parseDouble(path[1]);
        double Tmintemp = Double.parseDouble(path[2]);
        temperature = (Tmaxtemp + Tmintemp) / 2;
        if(Tmaxtemp != MISSING && Tmintemp != MISSING) {
            output.collect(new Text(year), new DoubleWritable(temperature));
        }
    }
}