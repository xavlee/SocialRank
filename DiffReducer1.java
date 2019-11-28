package edu.upenn.nets212.hw3;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DiffReducer1 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		double diff = 0;
		
		for (Text t : values) {
			diff = Math.abs(diff - Double.parseDouble(t.toString()));
		}
		
		context.write(key, new Text("" + diff)); //emit the key and its respective difference
	}
}
