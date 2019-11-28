package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String val = value.toString();
		String[] edge = val.split("\t"); //split on tab
		
		if (edge.length < 2) {
			return;
		}
		
		Text user = new Text(edge[0]); 
		Text follows = new Text(edge[1]);
		
		context.write(user, follows); //user follower adjacency list essentially
		context.write(follows, new Text("")); //to account for if the user doesn't follow anyone
	}
}
