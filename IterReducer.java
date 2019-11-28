package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		
		String follows = ""; //to preserve follower information
		Double rank = 0.0; //
		
		Double d = 0.15;
		
		for (Text t : value) {
			if (t.toString().contains("!")) { //if it is an adjacency list
				String[] s = t.toString().split("!");
				if (s.length > 1) {
					follows = "!" + s[1].toString();
				}
			} else {
				rank = rank + Double.parseDouble(t.toString()); //if it is a weight to be added
			}
		}
		
		rank = d + (1 - d) * rank; //enhanced surfer equation
		
		String val = "" + rank + follows;
		
		context.write(key, new Text(val)); //emit the same key and updated rank and orig adj list
	}
}
