package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinishMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\t"); //split on tabs
		
		if (line.length > 1) {
			String[] rankFollows = line[1].split("!");
			
			String user = line[0];
			double rank = Double.parseDouble(rankFollows[0]);
			
			context.write(new DoubleWritable(-1.0 * rank), new Text(user)); //write out ranks (sorted) and the user
		}
	}
}
