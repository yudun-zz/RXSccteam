

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

import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;

public class Q3_HBase {


    public static class HBaseMap extends Mapper<LongWritable, Text, Text,Text> {
    	
    	private Text OutKey = new Text();
		private Text OutValue = new Text();
    	
		@Override
    	public void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException, NumberFormatException {
		
			/*Deal with one json record*/
			String OneLine = value.toString();
			try{

				JSONObject jobj = new JSONObject(OneLine);
				
				jobj.remove("raw");
				jobj.remove("hash");
				
				String uid =  (String) jobj.get("uid");
				String time = (String) jobj.get("t");
				int impact = (int)jobj.getInt("impact");
				if(impact!=0){
					String wrapText = StringEscapeUtils.escapeJava((String)jobj.get("cen"));
					OutKey.set(uid);
					OutValue.set(uid+"+"+time+"\t"+(String)jobj.get("tid")+
						"\t"+impact+"\t"+wrapText);							
					
					context.write( OutKey, OutValue);
				}
			}
			catch(JSONException e){
				return;
			}
			
			
			return;
    					
		}
	}
	
    public static class HBaseReduce extends Reducer< Text,Text, Text,NullWritable> {

    	@Override
    	public void reduce( Text key, Iterable<Text> values, Context context) 
    				throws IOException, InterruptedException {
			 for (Text val : values) {
				context.write(val, NullWritable.get());
			}
		
    	}
    }
        
    public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();
   
    Job job = new Job(conf, "Q3_HBase");
    job.setJarByClass(Q3_HBase.class);
    job.setOutputKeyClass(Text.class);
  
    job.setMapperClass(HBaseMap.class);
    job.setReducerClass(HBaseReduce.class);
    
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
