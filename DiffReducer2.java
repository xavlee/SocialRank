package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DiffReducer2 extends Reducer<DoubleWritable, Text, Text, Text>{

	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		double maxDiff = -1.0 * key.get();
		
		context.write(new Text("" + maxDiff), new Text("")); //emit just the max diff
	}
}
