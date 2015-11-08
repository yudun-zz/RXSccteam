

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
import java.util.List;
import java.util.TreeSet;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;

public class Q4_HBase {

	static class datePack{
		String date;
		int count;
		TreeSet<Long> user_list;
		String earlest_ts;
		String earlest_tweet;
		
		datePack(String d){
			date = d;
			this.count = 0;
			user_list = new TreeSet<>();
			earlest_ts = "2023-05-12+00:00:00";
			earlest_tweet = "";
		}
	}
	
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
				
				jobj.remove("cen");
				jobj.remove("impact");
				
				String hash = (String) jobj.get("hash");
				String[] tags = hash.split(" ");
				jobj.remove("hash");
				
				for(int i=0;i<tags.length;i++){
					OutKey.set(tags[i]);
					OutValue.set(jobj.toString());
				
					context.write( OutKey, OutValue);
				}
				
			}
			catch(JSONException e){
				return;
			}
			
			
			return;
    					
		}
	}
	
    public static class HBaseReduce extends Reducer< Text,Text, Text,Text> {

		private Text OutValue = new Text();
    	@Override
    	public void reduce( Text key, Iterable<Text> values, Context context) 
    				throws IOException, InterruptedException {
			
			try{
			 HashMap<String,datePack> datemap = new HashMap<String,datePack>();
			 for (Text val : values) {
				JSONObject jobj = new JSONObject(val.toString());
				String time = (String) jobj.get("t");
				String date = time.split("\\+")[0];
				Long uid = jobj.getLong("uid");
				if(!datemap.containsKey(date))
					datemap.put(date, new datePack(date));
					
				datemap.get(date).count++;
				datemap.get(date).user_list.add(uid);
				
				if( time.compareTo(datemap.get(date).earlest_ts) < 0 ){
					datemap.get(date).earlest_ts = time;
					datemap.get(date).earlest_tweet = (String)jobj.get("raw");
				}
				else if( time.compareTo(datemap.get(date).earlest_ts)== 0 ){
					if(((String)jobj.get("raw")).compareTo(
							datemap.get(date).earlest_tweet) < 0){
						datemap.get(date).earlest_tweet = (String) jobj.get("raw");
					}
				}
				
			}
			
			List<datePack> dateList = new ArrayList<datePack>(datemap.values());
			Collections.sort(dateList, new Comparator<datePack>() {

		        public int compare(datePack o1, datePack o2) {
		            if(o2.count == o1.count)
		            	return o1.date.compareTo(o2.date);
		            return o2.count-o1.count;
		            
		        }
		    });
			
			JSONArray outarray = new JSONArray();
			for(datePack tmp : dateList){
				 JSONObject oneobj = new JSONObject();
				 Long[] users = tmp.user_list.toArray(new Long[tmp.user_list.size()]);
				 String users_str = users[0].toString();
				 
				 for(int i=1; i<users.length;i++ )
					users_str+=","+users[i];
				 
				 String str = tmp.date+":"+tmp.count+":"+users_str+":"+tmp.earlest_tweet;
				 oneobj.put("a",str);
				 outarray.put(oneobj);
			}
			OutValue.set(outarray.toString());
				
			context.write( key, OutValue);
			
			}
			catch(JSONException e){
				
			}
    	}
    }
        
    public static void main(String[] args) throws Exception {
	 
		Configuration conf = new Configuration();
	
		Job job = new Job(conf, "Q4_HBase");
		job.setJarByClass(Q4_HBase.class);
		job.setOutputKeyClass(Text.class);
	
		job.setMapperClass(HBaseMap.class);
		job.setReducerClass(HBaseReduce.class);
		job.setNumReduceTasks(30);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
			
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
		job.waitForCompletion(true);
	}
        
}
