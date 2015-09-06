package cn.terry.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class DCMapper extends MapReduceBase 
	implements Mapper<LongWritable,Text,Text,DataBean>{

	public void map(LongWritable key, Text value,
			OutputCollector<Text, DataBean> output, Reporter reporter)
			throws IOException {
		
	}



}
