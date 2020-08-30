package Questions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question3 {
	//mapper class
	public static class MapForWordCount extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value, Context con) throws IOException, InterruptedException
		{
			String line= value.toString();
			String[] words=line.split(",");
			Text outputkey=new Text(words[3]);
			IntWritable outputvalue=new IntWritable(1);
			con.write(outputkey,outputvalue);
			
			
		}
		
	}
	
	
	
	public static <K, V extends Comparable<V> > Map.Entry<K, V>
	getMaxEntryInMapBasedOnValue(Map<K, V> map)
	{
		
		//Storing the result
		Map.Entry<K, V> entrywithMaxValue = null;
		
		// Finding the desired entry in map
		for (Map.Entry<K, V> currentEntry : map.entrySet()) {
			
			if (
					
					entrywithMaxValue == null
					
					
					|| currentEntry.getValue()
					.compareTo(entrywithMaxValue.getValue()) > 0){
				entrywithMaxValue = currentEntry;
				
			}
					
					
					
		}
		return entrywithMaxValue;
		
	}
	
	
	
	//reducer class
	public static class ReduceForWordCount extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		Map<Text, IntWritable> map
		= new HashMap<>();
		public void reduce(Text word,Iterable<IntWritable> values,Context con)throws IOException, InterruptedException
		{
			
			int sum=0;
			for(IntWritable value:values)
			{
				sum=sum+value.get();
			}
			map.put(word,new IntWritable(sum));
			getMaxEntryInMapBasedOnValue(map);
			Text w1= getMaxEntryInMapBasedOnValue(map).getKey();
			IntWritable w2= getMaxEntryInMapBasedOnValue(map).getValue();
			con.write(w1, w2);
		}
		
		
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration c= new Configuration();
		Job j=Job.getInstance(c,"wordcount");
		j.setJarByClass(Question3.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j,new Path(args[0]));
		FileOutputFormat.setOutputPath(j,new Path(args[1]));
		System.exit(j.waitForCompletion(true)?0:1);
		
	}
	

}

