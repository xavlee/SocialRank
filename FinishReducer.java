package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinishReducer extends Reducer<DoubleWritable, Text, Text, Text> {
	
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		double rank = key.get() * -1.0; //get the rank, correct sign
		
		for (Text t : values) {
			String user = t.toString();
			context.write(new Text(user), new Text("" + rank)); //emit out the user and the rank.
		}
	}
}
