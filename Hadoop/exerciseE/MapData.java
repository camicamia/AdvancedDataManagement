package exerciseE;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				IntWritable, // Output key type
				Text> {// Output value type

// 	private static final IntWritable one = new IntWritable(1);
	protected IntWritable maxKey = new IntWritable(0);
    protected Text maxValue = new Text();
    
	protected void map(IntWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        if (key.get() > maxKey.get()) {
            maxKey = key;
            maxValue =new Text(value);
        }
    }
    protected void cleanup(Context context) throws IOException,InterruptedException {
            context.write(maxKey, maxValue);
    }
}

