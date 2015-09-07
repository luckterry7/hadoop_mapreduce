package cn.terry.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class DCMapper extends Mapper<LongWritable,Text,Text,DataBean>{

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] fields = line.split("\t");
		String tel = fields[1];
		try {
			long up = Long.parseLong(fields[8]);
			long down = Long.parseLong(fields[9]);
			DataBean dataBean = new DataBean(tel,up,down);
			//map输出的key，value
			context.write(new Text(tel), dataBean);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}




}
