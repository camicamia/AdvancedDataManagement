package exerciseH;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapTrans extends
		Mapper<IntWritable, // Input key type
				Text, // Input value type
				IntWritable, // Output key type
				ValuesWritable> {// Output value type

// 	private static final IntWritable one = new IntWritable(1);
    
	protected void map(IntWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        ctx.write(key, new ValuesWritable(value) );
    }
}

