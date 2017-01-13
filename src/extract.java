/*
*Extract Valid wikilinks and Generate Adjacency Graph
*
*@author Luoming Liu
*@param  input_path : path to the wikipedia data set i
*@param  output_path:directory where to store the result
*/


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class extract {
	private static Pattern pattern = Pattern.compile("\\[\\[(.*?)(\\||\\]\\])");
	private static String mark="#*&";
	

	/*
	*	MapReduce job that extracts wikilinks and also remove all the red links . The
	*	dataset contains, for each article, a list of links going out of that article, as well as the
	*	articles title. The input for this job should be the Wikipedia dataset, and the job
	*	should look at the XML text for each article, extract title and wikilinks.
	*/
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text>{
	    private Text word = new Text();
	    private Text wikilink=new Text();
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    		Document document = null;
	    		try{	
	    			document=DocumentHelper.parseText(value.toString());
	    		}catch(DocumentException e){
	    			e.printStackTrace();
	    		}
	    		word.set(document.selectSingleNode("/page/title").getText().replaceAll(" ", "_"));
	    		Matcher matcher = pattern.matcher(document.selectSingleNode("/page/revision/text").getText());
	    		wikilink.set(mark);
	    		context.write(word, wikilink);
	    		while (matcher.find()) {
	    			String tmp=matcher.group(1);
	    			wikilink.set(tmp.replaceAll(" ", "_"));
	    			if(wikilink.equals(word)) continue;
	    			context.write(wikilink,word);
	    		}
	    }
	}

	 public static class Reducer1 extends Reducer<Text,Text,Text,Text> {
	    private Text result = new Text();
	    
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	      boolean isredlink=true;
		      StringBuilder re=new StringBuilder();
		      for (Text val : values) {
		    	    String tmp=val.toString();
		    	    if(tmp.equals(mark)) {isredlink=false;}
			    re.append(tmp);
			    re.append(" ");
		      }
		      if(!isredlink){
		    	    result.set(re.toString());
			    context.write(key, result);
		      }
	    }
	  }
	  
	  /*
	  *	MapReduce job to generate the Adjacency graph, i.e a graph with the format:
	  *	page_title1 	link1 link2 …
	  *	page_titile2 	link2 link3 …
	  */
	 public static class Mapper2 extends Mapper<Object, Text, Text, Text>{
		 	private Text title=new Text();
		 	private Text link=new Text();
		    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    		Scanner in=null;
		    		try{
		    			in=new Scanner(value.toString());
		    		}catch(Exception e){e.printStackTrace();}
		    		while(in.hasNextLine()){
		    			Scanner line=new Scanner(in.nextLine());
		    			link.set(line.next());
		    			while(line.hasNext()){
		    				String tmp=line.next();
		    				if(tmp.equals(mark)) context.write(link,new Text(""));
		    				else{
			    				title.set(tmp);
			    				context.write(title, link);
		    				}
		    			}
		    			line.close();
		    		}
		    		if(in!=null) in.close();
		    }
	}

	public static class Reducer2 extends Reducer<Text,Text,Text,Text> {
		private Text links=new Text();
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 TreeSet<String> set=new TreeSet<>();
		      StringBuilder re=new StringBuilder();
		      for (Text val : values) {
		    	  	set.add(val.toString());
		      }
		      for(String a:set){
		    	  	re.append(a);
			        re.append("\t");
		      }
		      re.deleteCharAt(re.length()-1);
		      links.set(re.toString());
		      context.write(key, links);
		 }
	}

	  public static void main(String[] args) throws Exception {
	    final String Intermediate_PATH = args[1]+"/temp";
		  //job1: filter red link
	    Configuration conf1 = new Configuration();
	    conf1.set("xmlinput.start", "<page>");
	    conf1.set("xmlinput.end", "</page>");
	    Job job1 = Job.getInstance(conf1, "job1");
	    job1.setInputFormatClass(XMLInputFormat.class);
	    job1.setJarByClass(extract.class);
	    job1.setMapperClass(Mapper1.class);
	    job1.setReducerClass(Reducer1.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(Intermediate_PATH));
	    job1.waitForCompletion(true);
	    //job2: output page_titile : links
	    Configuration conf2 = new Configuration();
	    Job job2 = Job.getInstance(conf2, "job2");
	    job2.setJarByClass(extract.class);
	    job2.setMapperClass(Mapper2.class);
	    job2.setCombinerClass(Reducer2.class);
	    job2.setReducerClass(Reducer2.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, new Path(Intermediate_PATH));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/graph"));
	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
	  }
}
