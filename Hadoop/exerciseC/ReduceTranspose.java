package exerciseC;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceTranspose extends
		Reducer<IntWritable, // Input key type
				IntWritable, // Input value type
				IntWritable, // Output key type
				IntWritable> { // Output value type

  protected void reduce(IntWritable key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
    int count = 0;

    for(IntWritable value : values) {
      count += value.get();
		}

    ctx.write(key, new IntWritable(count));
    }
}
