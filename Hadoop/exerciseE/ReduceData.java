package exerciseE;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceData extends
		Reducer<IntWritable, // Input key type
				Text, // Input value type
				IntWritable, // Output key type
                Text> { // Output value type

    protected IntWritable maxKey = new IntWritable(0);
    protected Iterable<Text> maxValue;
    
    protected void setup(Context context){
        Configuration conf = context.getConfiguration();
    }
    protected void reduce(IntWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        int count = 0;
        if(key.get() > maxKey.get())
        {
                maxKey = key;
                maxValue = values;

        }
    }
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
            
        for(Text k: maxValue)
        {
            context.write(maxKey, k);
        }
  }
  
}
