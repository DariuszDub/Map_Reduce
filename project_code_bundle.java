/*Bellowss code provides count calls callculation per: District, Weekday, Month, Call_type 
  Target database need to be change and sorce column for each outpit  */
  
//Mapper
import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;

public class HBaseSummarisationMapper2 extends 
       TableMapper<Text, IntWritable> {
	
	private Text outVar= new Text();
	private IntWritable ONE = new IntWritable(1);
	
	public void map(ImmutableBytesWritable row, Result columns, Context context)
	       throws IOException, InterruptedException {
		
		String charAtoZ = new String(columns.getValue("cfid".getBytes(), "Day_of_week".getBytes()));
		outVar.set(charAtoZ);
		context.write(outVar, ONE);
	}

}


//Reducer

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;


public class HBaseSummarisationReducer2 extends
       TableReducer<Text, IntWritable, ImmutableBytesWritable>{
	

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	       throws IOException, InterruptedException {
		
		int sum = 0;
		
		for (IntWritable val : values) {
			sum += val.get();
		}
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.addColumn("cfid".getBytes(), "count".getBytes(), Bytes.toBytes(Integer.toString(sum)));
		
		context.write(null, put);
	}
	

}

//Driver

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;


public class HBaseSummarisationDriver2 {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		/*if (otherArgs.length != 0) {
		    System.err.println("Usage: HBaseSummarisationDriver2");
		    System.exit(2);
		}*/
		Scan scan = new Scan();
		Job job = Job.getInstance(conf, "Count frequency of Fld2 chars in HBase randomRows table.");
		job.setJarByClass(HBaseSummarisationDriver2.class);
		TableMapReduceUtil.initTableMapperJob("Project", scan, 
				                              HBaseSummarisationMapper2.class, 
				                              Text.class, IntWritable.class, 
				                              job);
		TableMapReduceUtil.initTableReducerJob("countPer", HBaseSummarisationReducer2.class, job);
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}

/*Bellows mapper allows count no. of calls for given district and type of callculation 
  reducer and driver as above */

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;

public class HBaseSummarisationMapper2 extends 
       TableMapper<Text, IntWritable> {
	
	private Text outVar= new Text();
	private IntWritable ONE = new IntWritable(1);
	
	public void map(ImmutableBytesWritable row, Result columns, Context context)
	       throws IOException, InterruptedException {
		
		String var = new String(columns.getValue("cfid".getBytes(), "Neighborhood_District".getBytes()));
		String var2 = new String(columns.getValue("cfid".getBytes(), "Call_Final_Disposition".getBytes()));
		//get district only for calls: Fire, Other ,Code 2
		if (var2.equals("Code 2 Transport") | var2.equals("Fire") | var2.equals("Other")){
			outVar.set(var + " - " + var2);
			context.write(outVar, ONE);
		}

	}

}
// maper tp produce eleveation and timestamp count

import java.io.IOException;

import java.util.Calendar;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;

public class HBaseSummarisationMapper2 extends 
       TableMapper<Text, IntWritable> {
	
	private Text outVar= new Text();
	private IntWritable ONE = new IntWritable(1);
	
	public void map(ImmutableBytesWritable row, Result columns, Context context)
	       throws IOException, InterruptedException {
		
		String var = new String(columns.getValue("cfid".getBytes(), "Elevation".getBytes()));
		String var2 = new String(columns.getValue("cfid".getBytes(), "Time_Diff".getBytes()));
		String var3 = new String(columns.getValue("cfid".getBytes(), "Location_x".getBytes()));
		String var4 = new String(columns.getValue("cfid".getBytes(), "Location_y".getBytes()));
		//Get rid of the outliers from time stamp and get location x and y
		if (var2.startsWith("0")){
			var2 = var2.replace("0 days ","");
			outVar.set(var2 + "," +var+ ","+var3+var4);
			context.write(outVar,ONE);
		}

	}

}

