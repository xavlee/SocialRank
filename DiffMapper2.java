package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DiffMapper2 extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] s = value.toString().split("\t"); //split on tabs
		
		if (s.length > 1) {
			double diff = Double.parseDouble(s[1]);
			
			context.write(new DoubleWritable(-1.0 * diff), new Text("")); //negative diff for ascending sorting
		}
	}
}