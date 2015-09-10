package cn.terry.hadoop.mapreduce.dc;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class DCReducer extends Reducer<Text,DataBean,Text,DataBean>{


	@Override
	protected void reduce(Text key, Iterable<DataBean> values,
			Context context)
			throws IOException, InterruptedException {
		long up_sum = 0;
		long down_sum = 0;
		
		for(DataBean bean:values){
			up_sum += bean.getUpPayLoad();
			down_sum += bean.getDownPayLoad();
		}
		//reduce输出格式
		context.write(new Text(key), new DataBean("",up_sum,down_sum));
	}


}
