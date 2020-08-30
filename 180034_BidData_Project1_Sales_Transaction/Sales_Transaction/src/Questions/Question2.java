package Questions;



import java.io.IOException;

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

public class Question2 {
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
	
	//reducer class
	public static class ReduceForWordCount extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text word,Iterable<IntWritable> values,Context con)throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable value:values)
			{
				sum=sum+value.get();
			}
			con.write(word,new IntWritable(sum));
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration c= new Configuration();
		Job j=Job.getInstance(c,"wordcount");
		j.setJarByClass(Question2.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j,new Path(args[0]));
		FileOutputFormat.setOutputPath(j,new Path(args[1]));
		System.exit(j.waitForCompletion(true)?0:1);
		
	}
	

}
