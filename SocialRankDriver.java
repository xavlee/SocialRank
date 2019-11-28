package edu.upenn.nets212.hw3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SocialRankDriver 
{
  public static void main(String[] args) throws Exception 
  {
	  if (args.length < 4 || args.length == 6 || args.length > 7) { //invalid number of arguments
		  return;
	  }
	  
	  String first = args[0];
	  
	  if (first.equals("composite")) { //composite
		  composite(args[1], args[2], args[3], args[4], args[5], args[6]);
	  } else if (first.equals("init")) { //init
		  init(args[1], args[2], args[3]);
	  } else if (first.equals("iter")) { //iter
		  iter(args[1], args[2], args[3]);
	  } else if (first.equals("diff")) { //diff
		  diff(args[1], args[2], args[3], args[4]);
	  } else if (first.equals("finish")) { //finish
		  finish(args[1], args[2], args[3]);
	  } else {
		  return;
	  }
		 
  }
  
  public static void init(String inputDir, String outputDir, String numR) throws Exception{
	  
	  Job job = new Job();
	  job.setJarByClass(SocialRankDriver.class);
	  
	  FileInputFormat.addInputPath(job, new Path(inputDir));
	  deleteDirectory(outputDir);
	  FileOutputFormat.setOutputPath(job, new Path(outputDir));
	  
	  job.setMapperClass(InitMapper.class);
	  job.setReducerClass(InitReducer.class);
	  
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(Text.class);
	  
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  
	  job.setNumReduceTasks(1);
	  
	  job.waitForCompletion(true);
  }
  
  public static void iter(String inputDir, String outputDir, String numR) throws Exception {
  
	  Job job = new Job(); //create job
	  job.setJarByClass(SocialRankDriver.class);
	  
	  FileInputFormat.addInputPath(job, new Path(inputDir));
	  deleteDirectory(outputDir);
	  FileOutputFormat.setOutputPath(job, new Path(outputDir));
	  
	  job.setMapperClass(IterMapper.class);
	  job.setReducerClass(IterReducer.class);
	  
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(Text.class);
	  
	  job.setNumReduceTasks(Integer.parseInt(numR));
	  
	  job.waitForCompletion(true);
  }
  
  public static void finish(String inputDir, String outputDir, String numR) throws Exception {
  
	  Job job = new Job(); //create job
	  job.setJarByClass(SocialRankDriver.class);
	  
	  FileInputFormat.addInputPath(job, new Path(inputDir));
	  deleteDirectory(outputDir);
	  FileOutputFormat.setOutputPath(job, new Path(outputDir));
	  
	  job.setMapperClass(FinishMapper.class);
	  job.setReducerClass(FinishReducer.class);
	  
	  job.setMapOutputKeyClass(DoubleWritable.class);
	  job.setMapOutputValueClass(Text.class);
	  
	  job.setNumReduceTasks(Integer.parseInt(numR));
	  
	  job.waitForCompletion(true);
  }
  
  public static void diff(String inputDir1, String inputDir2, String outputDir, String numR) throws Exception {
	  
	  String tmpOutput = "tmpOutput";
	  
	  Job job = new Job(); //create job
	  job.setJarByClass(SocialRankDriver.class);
	  
	    
      FileInputFormat.addInputPath(job, new Path(inputDir1));
      FileInputFormat.addInputPath(job, new Path(inputDir2));
      deleteDirectory(tmpOutput);
	  FileOutputFormat.setOutputPath(job, new Path(tmpOutput));
	    
      job.setMapperClass(DiffMapper1.class);
      job.setReducerClass(DiffReducer1.class);
	    
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	    
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
	    
      job.setNumReduceTasks(Integer.parseInt(numR));
	    
      if (job.waitForCompletion(true)) { //wait for job to finish before starting diff2
	      job = new Job();
	      job.setJarByClass(SocialRankDriver.class);
		  
	  	  FileInputFormat.addInputPath(job, new Path(tmpOutput));
		  deleteDirectory(outputDir);
		  FileOutputFormat.setOutputPath(job, new Path(outputDir));
		  
		  job.setMapperClass(DiffMapper2.class);
		  job.setReducerClass(DiffReducer2.class);
		  
		  job.setMapOutputKeyClass(DoubleWritable.class);
		  job.setMapOutputValueClass(Text.class);
		  
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  
		  job.setNumReduceTasks(Integer.parseInt(numR));
		  
		  job.waitForCompletion(true);
      }
  }
  
  public static void composite(String inputDir, String outputDir, String intermDir1, String intermDir2, String diffDir, String numR) throws Exception {
	  
	  int count = 0; //count used to keep track of number of iterations
	  double tolerance = 30; //tolerance level
	  double difference = Double.MAX_VALUE; //difference
	  
	  init(inputDir, intermDir1, numR);
	  
	  while (difference > tolerance) { //checks if diff is below tolerance
		  
		  if (count % 2 == 0) { //switch iter directories, run iter
			  iter(intermDir1, intermDir2, numR);
		  } else {
			  iter(intermDir2, intermDir1, numR);
		  }
		  
		  count++;
		  
		  if (count % 3 == 0) { //check diff every 3 iterations.
			  diff(intermDir1, intermDir2, diffDir, numR);
			  difference = readDiffResult(diffDir);
		  }
	  }
	  
	  if (count % 2 == 0) { //finish the job
		  finish(intermDir1, outputDir, numR);
	  } else {
		  finish(intermDir2, outputDir, numR);
	  }
	  
	  System.out.println("Solution by: Xavier Lee (xavlee)");
  }

  // Given an output folder, returns the first double from the first part-r-00000 file
  static double readDiffResult(String path) throws Exception 
  {
    double diffnum = 0.0;
    Path diffpath = new Path(path);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(path),conf);
    
    if (fs.exists(diffpath)) {
      FileStatus[] ls = fs.listStatus(diffpath);
      for (FileStatus file : ls) {
	if (file.getPath().getName().startsWith("part-r-00000")) {
	  FSDataInputStream diffin = fs.open(file.getPath());
	  BufferedReader d = new BufferedReader(new InputStreamReader(diffin));
	  String diffcontent = d.readLine();
	  diffnum = Double.parseDouble(diffcontent);
	  d.close();
	}
      }
    }
    
    fs.close();
    return diffnum;
  }

  static void deleteDirectory(String path) throws Exception {
    Path todelete = new Path(path);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(path),conf);
    
    if (fs.exists(todelete)) 
      fs.delete(todelete, true);
      
    fs.close();
  }

}
