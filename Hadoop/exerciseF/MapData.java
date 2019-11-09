package exerciseF;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapData extends
		Mapper<IntWritable, // Input key type
                Text, // Input value type
				IntWritable, // Output key type
				Text> {// Output value type

	
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
	protected void map(IntWritable key, Text value, Context ctx) throws IOException, InterruptedException {
	
        if (key.get() < minKey.get() || minKey.get() == 0) 
        {
            minKey.set(key.get());
            minValue.set(value);
        }
        
        if (key.get() > maxKey.get()) {
            maxKey.set(key.get());
            maxValue.set(value);
        }
        
    }
    protected void cleanup(Context context) throws IOException,InterruptedException {
           
            if(minKey.get() == maxKey.get())
                context.write(minKey,minValue); 
            
            else
            {
                context.write(minKey, minValue);
                context.write(maxKey, maxValue);
            }
    }
}

