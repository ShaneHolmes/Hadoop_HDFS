package com.shane.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * MapReudce的入门程序： wordcount
 * 
 
数据：

hello huangbo
hello xuzheng
hello wangbaoqiang
1 1 1 1 1 
2 2 2 2
3 3 3
4 4
5

 *
 */
public class WordCountMR {

	public static void main(String[] args) throws Exception {

		
		Configuration conf = new Configuration();

		conf.set("fs.defaultFS", "hdfs://192.168.65.101:9000");
		System.setProperty("HADOOP_USER_NAME","hadoop");
		//conf.set("HADOOP_USER_NAME","hadoop");
		//conf.set("fs.defaultFS", "file:///");
//		System.out.println(conf.get("fs.defaultFS"));
		
		/**
		 * 获取一个job对象。 一个job就是一个完整的MapReduce APP   应用程序
		 * 
		 * job  一次工作
		 * task 一次任务
		 */
		Job job = Job.getInstance(conf);
		
//		job.setJar("/home/hadoop/wc.jar");
		job.setJarByClass(WordCountMR.class);
		
		
		/**
		 * 设置 该 job 的  mapper和 reducer组件
		 */
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		
		/**
		 * 为什么要设置  job  mapper组件的输出的key-value的类型?
		 * 
		 * 因为 泛型 存在着  擦除的概念。  
		 * 
		 * 	      泛型只在编译生效
		 *    编译好了之后的class文件中，是压根没有泛型的概念
		 *    
		 *  因为既然在网络传输过程当中存在序列化的操作， 那么要存在反操作， 反序列化操作
		 *  
		 *  为什么只需要指定输出的泛型 ，  而不是指定输入的泛型？
		 *  
		 *  因为mapper组件的输入的key-value的类型是跟随 数据读取组件来决定 
		 *  
		 *  现在的wordcount程序的默认数据读取组件是 TextInputFormat和LineRecordReader
		 *  他们已经指定好了  mapper组件的输入的key-value的类型是  LongWritable和Text
		 */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		/**
		 * 为什么不要指定 reducer的输入的key-value类型？
		 * 
		 * 因为reducer组件的输入的key-value的类型是由mapper组件的输出决定。
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		/**
		 * 设置reduceTsk的个数
		 * 
		 * 1、没有设置，  默认运行 1 个 reduceTask
		 * 
		 * 2、在默认的HashPartitioner的规则之下，  reduceTask的个数可以随意设置。
		 * 
		 * 3、如果自定义了 Paritioner组件， 那么设置 redcueTASK的个数就必须结合具体的业务
		 * 	
		 * 4、设置reduceTask为0 个。。那就表示  根本就没有reducer阶段。
		 *   所以mapper阶段的数据直接输出 成为最终的结果
		 *
		 * 5、如果没有设置reducer组件， 又没有指定reducetask的个数， 那就表示会运行一个reduceTask
		 *    运行默认的实现。
		 *    
		 *    
		 *   第四种方式 和 第五种方式 ：
		 *   
		 *   		所有maptask的结果数据都被原样输出。
		 *   
		 *   		但是又不一样的地方：   如果没有reducer阶段， 那就表示，  mapredcue程序不会对key-value进行排序
		 *          就表示咩有  mapper和reducer中间的shuffle阶段
		 *          
		 */
		job.setNumReduceTasks(1);   //  reduceTask的编号是： 0  1  2  
		
		// 如果有某一个值的getPartition方法的返回值不在这个取值范围之内，就会报错。！！！！！
		
		
		/**
		 * 指定该job的输入输出数据的路径
		 */

        FileInputFormat.setInputPaths(job, args[0]);
		//FileInputFormat.setInputPaths(job, new Path("/wc/input/"));

		Path outputPath = new Path(args[1]);
//		Path outputPath = new Path("/wc/output/");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		/**
		 * 提交任务去运行
		 * 
		 * 提交任务到YARN集群去运行
		 * 
		 * 提交的方法可以是；
		 * 
		 * 	job.submit();
		 *  job.waitForCompletion(true);
		 *  
		 *  true ：是否打印执行过程
		 *  
		 *  当前这个方式是一个阻塞方法。
		 */
		boolean isDone = job.waitForCompletion(true);
		
		/**
		 * 这是退出整个MapReduce程序的命令
		 * 
		 * 如果传参为0， 表示正常退出。
		 * 如果不是0， 那就表示异常退出
		 */
		System.exit(isDone ? 0 : 1);
		
	}
}
