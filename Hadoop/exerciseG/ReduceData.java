package exerciseG;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceData extends
		Reducer<IntWritable, // Input key type
				IntWritable, // Input value type
				FloatWritable, // Output key type
                NullWritable>  // Output value type
{
    protected int keySum;
    protected int valueSum;
    
    protected void setup(Context context)
    {
        keySum =0;
        valueSum = 0;
    }
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException 
    {
        //IntWritable temp = value.iterator().next();
        //add each keySum of mapper
        for(IntWritable temp: values)
        {
            keySum += key.get();
            valueSum += temp.get();
        }
        
    }
    protected void cleanup(Context context) throws IOException,
            InterruptedException 
    {
            
            float keySumFloat = (float)keySum;
            float valueSumFloat = (float)valueSum;
            FloatWritable avg = new FloatWritable(keySumFloat/valueSumFloat);
            context.write(avg,NullWritable.get());
    }
  
}
