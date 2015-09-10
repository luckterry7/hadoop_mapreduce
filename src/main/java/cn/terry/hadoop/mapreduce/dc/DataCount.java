package cn.terry.hadoop.mapreduce.dc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DataCount {
	
	public static void main(String[] args) throws Exception {
		//设置个性的conf
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		// 设置main函数的class
		job.setJarByClass(DataCount.class);
		
		job.setMapperClass(DCMapper.class);
		//设定k2，v2
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataBean.class);
		
		//设定数据读取的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(DCReducer.class);
		//设定k3，v3
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
		
		//设定mr之后的输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//执行并打印详情
		job.waitForCompletion(true);
	}
}
