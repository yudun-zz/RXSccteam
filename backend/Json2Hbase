
  
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.lang.Byte;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.json.JSONObject;
import org.json.JSONException;
import java.io.BufferedReader;
import java.text.ParseException;




public class HbaseGenerator2 {


	static class HMap extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		private long ts;

		static byte[] family = Bytes.toBytes("TweetFamily");

		@Override
		protected void setup(Context context) {
				ts = System.currentTimeMillis();
		}
		
		@Override
		public void map(LongWritable offset, Text value, Context context)
			throws IOException {
			try {
		
			String OneLine = value.toString();
		
			JSONObject jobj = new JSONObject(OneLine);
			String tid_key = (String) jobj.get("tid");
			String uid_value = (String) jobj.get("uid");
			String text_value = (String) jobj.get("text");
			String t_value = (String) jobj.get("t");
			int score = (int) jobj.get("score");
			String score_value = String.valueOf(score);
		

			ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(tid_key));

			
			Put record_put = new Put(Bytes.toBytes(tid_key));
			record_put.add(family,Bytes.toBytes("uid"), ts,  Bytes.toBytes(uid_value) );
			record_put.add(family,Bytes.toBytes("t"), ts, Bytes.toBytes(t_value));
			record_put.add(family,Bytes.toBytes("text"), ts, Bytes.toBytes(text_value));
			record_put.add(family,Bytes.toBytes("score"), ts, Bytes.toBytes(score_value) );
		
			context.write(rowKey, record_put);
			
			} catch (InterruptedException e) {
				e.printStackTrace();
			}catch(JSONException e){
				return;
			}
			
		}
	}
	
	public static Job createSubmittableJob(Configuration conf, String[] args)
		throws IOException {
	
		String tableName = args[0];
		Path inputDir = new Path(args[1]);
	
		Job job = new Job(conf, "MapReduceImportJob");
		job.setJarByClass(HbaseGenerator2.class);
		FileInputFormat.setInputPaths(job, inputDir);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(HMap.class);
	
		if (args.length < 3) {
			TableMapReduceUtil.initTableReducerJob(tableName, null, job);
			job.setNumReduceTasks(0);
		} 
		else {
				/*生成HFile使用下面驱动
			HTable table = new HTable(conf, tableName);
			job.setReducerClass(PutSortReducer.class);
			Path outputDir = new Path(args【2】);
			FileOutputFormat.setOutputPath(job, outputDir);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);
			HFileOutputFormat.configureIncrementalLoad(job, table);
			*/ }
	
		TableMapReduceUtil.addDependencyJars(job);
		return job;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = createSubmittableJob(conf, args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}	
