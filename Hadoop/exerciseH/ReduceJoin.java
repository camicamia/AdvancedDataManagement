package exerciseH;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceJoin extends
		Reducer<IntWritable, // Input key type
				ValuesWritable, // Input value type
				IntWritable, // Output key type
				Text> { // Output value type
    
	protected void reduce(IntWritable key, Iterable<ValuesWritable> values, Context ctx) throws IOException, InterruptedException {
        String temp = new String();
        int newKey = 0;
        boolean check_len = false;
        boolean check_words = false;
        
		for(ValuesWritable v:  values) {
            if(v.isWords()){
                check_words = true;
                temp = temp + " " + v.getWords().toString();
            }
            else if(v.isLen()){
                check_len = true;
                newKey = v.getLen().get();
            }
		}
        if(check_words && check_len)
        ctx.write(new IntWritable(newKey), new Text(temp));
	}
}
