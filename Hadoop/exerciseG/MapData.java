package exerciseG;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapData extends
		Mapper<IntWritable, // Input key type
				IntWritable, // Input value type
				IntWritable, // Output key type
				IntWritable> // Output value type
{

	
	//sum of the key and sum of the value of the mapper
	protected int keySum;
    protected int valueSum;
    
    protected void setup(Context context)
    {
        keySum = 0;
        valueSum = 0;
    }
    
	protected void map(IntWritable key, IntWritable value, Context ctx) throws IOException, InterruptedException
	{
	
//         int multiply = key.get()*value.get();
        keySum += key.get()*value.get();
        valueSum += value.get();
        
    }
    protected void cleanup(Context context) throws IOException,InterruptedException 
    {
            IntWritable keySumW = new IntWritable(keySum);
            IntWritable valueSumW = new IntWritable(valueSum);
            
            context.write(keySumW, valueSumW);
    }
}

