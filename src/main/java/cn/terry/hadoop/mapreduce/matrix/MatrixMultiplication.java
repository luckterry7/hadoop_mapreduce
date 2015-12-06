package cn.terry.hadoop.mapreduce.matrix;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MatrixMultiplication {
	
	private static Logger logger=LoggerFactory.getLogger(MatrixMultiplication.class); 
	
	public static void main(String[] args) throws Exception {
		//设置个性的conf
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		// 设置main函数的class
		job.setJarByClass(MatrixMultiplication.class);
		
		job.setMapperClass(MatrixMapper.class);
		//设定k2，v2
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//设定数据读取的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(MatrixReducer.class);
		//设定k3，v3
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设定mr之后的输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//执行并打印详情
		job.waitForCompletion(true);
	}
	
	static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text>{

		private String flag;// m1 or m2

        private int rowNum = 2;// 矩阵A的行数
        private int colNum = 2;// 矩阵B的列数
        private int rowIndexA = 1; // 矩阵A，当前在第几行
        private int rowIndexB = 1; // 矩阵B，当前在第几行
        
        
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			 FileSplit split = (FileSplit) context.getInputSplit();
	         flag = split.getPath().getName();// 判断读的数据集
	         System.out.println("setup flag:" + flag);
		}


		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
				String[] tokens = value.toString().split(",");
				if (flag.equals("m1.txt")) {
					logger.info(new String("执行m1.txt的mapper".getBytes(),"utf-8"));
	                for (int i = 1; i <= rowNum; i++) {
	                    Text k = new Text(rowIndexA + "," + i);
	                    for (int j = 1; j <= tokens.length; j++) {
	                        Text v = new Text("A:" + j + "," + tokens[j - 1]);
	                        context.write(k, v);
	                        System.out.println(k.toString() + "  " + v.toString());
	                    }
	
	                }
	                rowIndexA++;
	                
	                
				} else if (flag.equals("m2.txt")) {
					logger.info(new String("执行m2.txt的mapper".getBytes(),"utf-8"));
	                for (int i = 1; i <= tokens.length; i++) {
	                    for (int j = 1; j <= colNum; j++) {
	                        Text k = new Text(i + "," + j);
	                        Text v = new Text("B:" + rowIndexB + "," + tokens[j - 1]);
	                        context.write(k, v);
	                        System.out.println(k.toString() + "  " + v.toString());
	                    }
	                }
	
	                rowIndexB++;
				}
			}
		
	}
	
	static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			Map<String, String> mapA = new HashMap<String, String>();
            Map<String, String> mapB = new HashMap<String, String>();
            logger.info(new String("执行的reducer".getBytes(),"utf-8"));
            System.out.print(key.toString() + ":");

            for (Text line : values) {
                String val = line.toString();
                System.out.print("("+val+")");

                if (val.startsWith("A:")) {
                    String[] kv = (val.substring(2).split(","));
                    mapA.put(kv[0], kv[1]);

                    // System.out.println("A:" + kv[0] + "," + kv[1]);

                } else if (val.startsWith("B:")) {
                    String[] kv = (val.substring(2).split(","));
                    mapB.put(kv[0], kv[1]);

                    // System.out.println("B:" + kv[0] + "," + kv[1]);
                }
            }

            int result = 0;
            Iterator<String> iter = mapA.keySet().iterator();
            while (iter.hasNext()) {
                String mapk = iter.next();
                result += Integer.parseInt(mapA.get(mapk)) * Integer.parseInt(mapB.get(mapk));
            }
            context.write(key, new IntWritable(result));
            System.out.println();

            // System.out.println("C:" + key.toString() + "," + result);
        }
	}
}
