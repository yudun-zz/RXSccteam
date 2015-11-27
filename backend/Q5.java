

import java.io.IOException;
import java.util.*;
import java.lang.Override;
import java.math.BigInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;


public class Q5 {


    public static class ParserMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    	
    	private Text OutKey = new Text();
  
    	
	@Override
    	public void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException, NumberFormatException {
		
			/*Deal with one json record*/
			String OneLine = value.toString();
			try{

				JSONObject jobj = new JSONObject(OneLine);
				
				/*Parsing and converting time*/
				String time = (String) jobj.get("created_at");
				
				SimpleDateFormat InFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy",Locale.ENGLISH); 
				InFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
				SimpleDateFormat OutFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss"); 
				OutFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
				Date CreateTime = InFormat.parse(time);
				time = OutFormat.format(CreateTime).toString();
				if(!CreateTime.before(OutFormat.parse("2014-04-20+00:00:00")))
					return;
				
				/*Parsing UserID*/
				JSONObject userobj = (JSONObject) jobj.get("user");
				String userid = (String)userobj.get("id_str");
				
			
				OutKey.set(userid);
				context.write(OutKey, OutValue);							
				
			}
			catch(JSONException e){
				return;
			}
			catch(ParseException e){
				return;
			}
			
			return;
    					
		}
	    
}
        
    public static class ParserReduce extends Reducer<Text,IntWritable,Text,IntWritable> {

		private IntWritable sumValue = new IntWritable();
		
    	@Override
    	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
    				throws IOException, InterruptedException {
    		
			/*Sum up count for each key*/
			int count = 0;
			
			for (IntWritable val : values) {
				count += val.get();
			}			
			sumValue.set(count);
			context.write(key, sumValue);
	
			
    	}
    }
        
    public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();

    Job job = new Job(conf, "Q5");
    job.setJarByClass(Q5.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
	
    job.setMapperClass(ParserMap.class);
    job.setReducerClass(ParserReduce.class);
   
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
