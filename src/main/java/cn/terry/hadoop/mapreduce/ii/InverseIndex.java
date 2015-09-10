package cn.terry.hadoop.mapreduce.ii;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InverseIndex {
	
	public static class IndexMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		//用于作为mapper的输出
		private Text k = new Text();
		private Text v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] words = line.split(" ");
			//从context中得到每个文件
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			//得到文件的路径,例如hdfs://terry01:9000/ii/a.txt
			String path = inputSplit.getPath().toString();
			
			for(String w : words){
				//把单词和文件路径拼接在一起
				k.set(w + "->" + path);
				//同时记录v为1
				v.set("1");
				context.write(k, v);
			}
		}
		
	}
	
	
	public static class IndexCombiner extends Reducer<Text,Text,Text,Text>{

		//用于作为reducer的输出
		private Text k = new Text();
		private Text v = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			//把单词和路径分割出来
			String[] wordsAndPath = key.toString().split("->");
			String word = wordsAndPath[0];
			String path = wordsAndPath[1];

			//定义计数器
			int counter = 0;
			
			for(Text t : values){
				counter += Integer.parseInt(t.toString());
			}
			//设定单词为key
			k.set(word);
			
			//设定path+出现次数 为value
			v.set(path + "->" + counter);
			context.write(v, v);
		}
	}

	public static class IndexReducer extends Reducer<Text,Text,Text,Text>{

		private Text v = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			StringBuilder result = new StringBuilder();
			for(Text t: values){
				result = result.append(t.toString() + "\t");
			}
			
			v.set(result.toString());
			
			context.write(key, v);
		}
		
		
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(InverseIndex.class);
		
		job.setMapperClass(IndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setCombinerClass(IndexCombiner.class);
		
		job.setReducerClass(IndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
