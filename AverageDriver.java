import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobClient;

public class AverageDriver
{

    public static void main (String[] args) throws Exception
    {
        long StartTime = System.currentTimeMillis();
        if (args.length != 5)
        {
            System.err.println("Usage: MaxTemperature <input path> <output path> <number of mapper> <number of reducer> <yearly or seasonally>");
            System.exit(-1);
        }

        JobConf conf = new JobConf(AverageDriver.class);
        conf.setJobName("Max temperature");
        conf.setNumMapTasks(Integer.parseInt(args[2]));
        conf.setNumReduceTasks(Integer.parseInt(args[3]));

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        if(args[4].equalsIgnoreCase("yearly")) {
            conf.setMapperClass(AverageMapper.class);
        }
        else {
            conf.setMapperClass(SeasonalAverageMapper.class);
        }
        conf.setReducerClass(AverageReducer.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(DoubleWritable.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        JobClient.runJob(conf);
        long EstimatedTime = System.currentTimeMillis() - StartTime;
        System.out.println("Time taken: "+EstimatedTime);
    }
}