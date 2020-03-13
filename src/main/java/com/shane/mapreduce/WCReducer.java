package com.shane.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  当前的WCMapper组件就是WordCountMR中的   第一个阶段的  reduce 函数的定义之地
 *  
 *  
 *  四个泛型的含义：
 *  
 *  前一对：  reduce组件输入的key-value的类型
 *  
 *  后一对： reduce组件输出的key-value的类型
 *  
 * @param <KEYIN> the class of the input keys
 * @param <VALUEIN> the class of the input values
 * @param <KEYOUT> the class of the output keys
 * @param <VALUEOUT> the class of the output values
 *  
 */


/**
 * reducer组件的输入key-value的类型 必须 和  mapper组件的 输出的key-value类型要一致
 *
 */
public class WCReducer extends Reducer<Text, IntWritable, Text, LongWritable>{

	/**
	 * reduce方法的参数：
	 * 	
	 * key ： 单词
	 * values ： 单词的次数的一个集合 （是唯一的一个所有的相同单词的value集合）
	 * 
	 *  key : hello
	 *  values （1,1,1,1,1,1）
	 *  
	 *  reduce方法每执行一次，接收到的参数，就是所有  key相同的 key-value的集合
	 *  
	 *  每次shuffle过程处理完了一个key所对应的所有的key-value之后，就会调用 reducer组件的reduce方法 执行一次  聚合操作（业务逻辑）
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		for(IntWritable value : values){
			
			sum += value.get();
		}
		
		// 输出结果
		context.write(key,  new LongWritable(sum));
	}
}
