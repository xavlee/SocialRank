package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InitReducer extends Reducer<Text, Text, Text, Text >{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String follows = ""; //following string
		
		for (Text s : values) { 
			if (!s.toString().equals("")) { //if the person has friends
				follows = follows + "," + s;
			}
		}
		
		if (follows.length() > 1) { //if the person has friends
			follows = follows.substring(1);
			follows = "1!" + follows;
		} else {
			follows = "1!";
		}
		
		
		Text val = new Text(follows);
		
		context.write(key, val); //user, rank, and adjacency list
	}
}