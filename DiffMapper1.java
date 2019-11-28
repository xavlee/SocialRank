package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DiffMapper1 extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String s = value.toString();
		
		String[] user = s.split("\t");
		
		if (user.length > 1) {
			String[] rankFollows = user[1].split("!");
			
			context.write(new Text(user[0]), new Text(rankFollows[0])); //write the user and rank.
		}
	}
}
