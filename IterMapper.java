package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		
		String[] userInfo = line[1].split("!");
		
		if (userInfo.length < 2) { // user has no friends
			return;
		}
		
		String[] follows = userInfo[1].split(","); //split up the followers
		double rank = Double.parseDouble(userInfo[0]);
		double numFollows = follows.length;
		
		double weight = rank / numFollows; //determine weight
		
		Text weightToAdd = new Text("" + weight);
		
		for (int i = 0; i < follows.length; i++) {
			Text userWeightAdd = new Text(follows[i]);
			
			context.write(userWeightAdd, weightToAdd); //emit the user to add weight and the weight
		}
		
		context.write(new Text(line[0]), new Text("!" + userInfo[1])); //adjacency list to preserve into
		
	}
}
