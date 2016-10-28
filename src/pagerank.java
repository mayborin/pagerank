/* 
* This is the pagerank class that complete the page rank task. We do this task after we have got the adjacent graph
* from @see (extract.java). 
* The output file includes:
*		 _______
*	 			|_______num_nodes -- contains the number of nodes N (page titles) calculated from input graph.
*				|_______iter1.out --- contains result after 1st iteration of pagerank score calculation.
*				|_______iter8.out --- contains result after 8th iteration of pagerank score calculation.
*				|_______temp/ -- contains intermediate files that will be ignored by TA.
* 
* @author Luoming Liu
* @param input_path  : the path to the adjacent graph files extracted from wiki data set
* @param output_path : the path to the directory where to store the result
*/
import java.io.IOException;
import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;



public class pagerank {
	private final static double d=0.85;
	private static int n=0;// number of pages


	public static class CountMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text("word");

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	      Scanner in=new Scanner(value.toString());
	      while (in.hasNextLine()) {
	        context.write(word, one);
	        in.nextLine();
	      }
	      if(in!=null) in.close();
	    }
	}
	
	public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
			 sum += val.get();
			}
			context.write(new Text(""), new IntWritable(sum));
		}
	}
	
	public static class IniMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Scanner in=new Scanner(value.toString());
			double inipr=1;
			while(in.hasNextLine()){
				Scanner line=new Scanner(in.nextLine());
				Text page=new Text(line.next());
				int tmpn=0;
				List<String> outlinks=new ArrayList<String>();
				while(line.hasNext()){
					tmpn++;
					String link=line.next();
					context.write(page, new Text("#"+link));
					outlinks.add(link);
				}
				double linkweight=inipr/tmpn;
				for(String a : outlinks) context.write(new Text(a), new Text(String.valueOf(linkweight)));
				if(line!=null) line.close();
			}
			if(in!=null) in.close();
		}
	}
	
	public static class IniReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			double sum=0;
			StringBuilder tmp=new StringBuilder();
			for(Text a:values){
				String aa=a.toString();
				if(aa.charAt(0)=='#') {
					tmp.append(aa.substring(1));
					tmp.append(" ");
				}
				else sum=sum+Double.parseDouble(aa);
			}
			sum =sum*d;
			sum =sum+1-d;
			tmp.insert(0, sum+" ");
			context.write(key, new Text(tmp.toString()));
		}
	}
	
	public static class InterMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Scanner in=new Scanner(value.toString());
			while(in.hasNextLine()){
				Scanner line=new Scanner(in.nextLine());
				Text page=new Text(line.next());
				int tmpn=0;
				List<String> outlinks=new ArrayList<>();
				double pr=Double.parseDouble(line.next());
				while(line.hasNext()){
					tmpn++;
					String link=line.next();
					context.write(page, new Text("#"+link));
					outlinks.add(link);
				}
				double linkweight=pr/tmpn;
				for(String a : outlinks) context.write(new Text(a), new Text(String.valueOf(linkweight)));
				if(line!=null) line.close();
			}
			if(in!=null) in.close();
		}
	}
	
	public static class sortMapper extends Mapper<Object, Text, DoubleWritable, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Scanner in=new Scanner(value.toString());
			while(in.hasNextLine()){
				Scanner line=new Scanner(in.nextLine());
				Text page=new Text(line.next());
				DoubleWritable pr=new DoubleWritable(Double.parseDouble(line.next()));
				context.write(pr, page);
				if(line!=null) line.close();
			}
			if(in!=null) in.close();
		}
	}
	public static class sortReducer extends Reducer<DoubleWritable, Text, Text, Text>{
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			double cut=5;
			if(key.get()>=cut){
				for(Text a: values){
					context.write(a, new Text(String.format("%.20f", key.get()/n)));
				}
			}	
		}
	}
	
	/* 
	* override the default sorter to sort in decreasing order(extends WritableComparator).
	*/
	
	public static class myComparator extends WritableComparator{
		//constructor
		protected myComparator(){
			super(DoubleWritable.class, true);
		}
		
		//override
		public int compare(WritableComparable w1, WritableComparable w2){
			DoubleWritable k1=(DoubleWritable) w1;
			DoubleWritable k2=(DoubleWritable) w2;
			return -1*k1.compareTo(k2);
		}
	}
	public static void calN(String input_path, String output_path) throws Exception{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "job CalN");
	    job.setJarByClass(pagerank.class);
	    job.setMapperClass(CountMapper.class);
	    job.setReducerClass(CountReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(input_path));
	    FileOutputFormat.setOutputPath(job, new Path(output_path+"nodes"));
	    job.waitForCompletion(true);
	    //Merge output to one file
	    Path srcPath=new Path(output_path+"nodes");
	    Path destPath=new Path(output_path+"/num_nodes");
	    FileSystem srcFs=srcPath.getFileSystem(conf);
	    FileSystem destFs=destPath.getFileSystem(conf);
	    FileUtil.copyMerge(srcFs, srcPath, destFs, destPath, true, conf, null);
	    Scanner in=new Scanner(new File(output_path+"/num_nodes"));
		n=in.nextInt();
		in.close();
	}
	public static void iterate(String input_path, String output_path) throws Exception{
		//Initial iterate, only do for once
		do{
			Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "job Iterate");
		    job.setJarByClass(pagerank.class);
		    job.setMapperClass(IniMapper.class);
		    job.setReducerClass(IniReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(input_path));
		    FileOutputFormat.setOutputPath(job, new Path(output_path+"/temp/tmp1"));
		    job.waitForCompletion(true);
		}while(false);
	    
		for(int i=2;i<=8;i++){
			Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "job Iterate");
		    job.setJarByClass(pagerank.class);
		    job.setMapperClass(InterMapper.class);
		    job.setReducerClass(IniReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(output_path+"/temp/tmp"+(i-1)));
		    FileOutputFormat.setOutputPath(job, new Path(output_path+"/temp/tmp"+i));
		    job.waitForCompletion(true);
		}
	}

	public static void sort(String in_path, String out_path) throws Exception{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "job Sort");
	    job.setJarByClass(pagerank.class);
	    job.setMapperClass(sortMapper.class);
	    job.setReducerClass(sortReducer.class);
	    job.setSortComparatorClass(myComparator.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(1);
	    job.setMapOutputKeyClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(in_path));
	    FileOutputFormat.setOutputPath(job, new Path(out_path));
	    job.waitForCompletion(true);
	    //Merge output to one file
	    Path srcPath=new Path(out_path);
	    Path destPath=new Path(out_path+".out");
	    FileSystem srcFs=srcPath.getFileSystem(conf);
	    FileSystem destFs=destPath.getFileSystem(conf);
	    FileUtil.copyMerge(srcFs, srcPath, destFs, destPath, true, conf, null);
	}
	public static void main(String[] args) throws Exception{
		pagerank.calN(args[0], args[1]);
		pagerank.iterate(args[0], args[1]);
		pagerank.sort(args[1]+"/temp/tmp1", args[1]+"/iter1");
		pagerank.sort(args[1]+"/temp/tmp8", args[1]+"/iter8");
	}
}
