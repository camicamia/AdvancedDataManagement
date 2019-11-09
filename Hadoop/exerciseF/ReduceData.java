package exerciseF;

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

    protected IntWritable maxKey;
    protected Text maxValue;
    
    protected IntWritable minKey;
    protected Text minValue;
    
    protected void setup(Context context){
        maxKey = new IntWritable(0);  
        maxValue = new Text();
        minKey = new IntWritable(0);
        minValue = new Text();
    }
    protected void reduce(IntWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        
        Text temp = values.iterator().next();
        if(key.get() < minKey.get() || minKey.get() == 0)
        {
                minKey.set(key.get());
                minValue.set(temp);
                
        }
        if(key.get() > maxKey.get())
        {
                maxKey.set(key.get());
                maxValue.set(temp);
        }
        
    }
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        
            context.write(minKey,minValue);
            context.write(maxKey, maxValue);
        
        
  } 
  
}
