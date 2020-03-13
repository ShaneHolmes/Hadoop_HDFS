package com.shane.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 当前的WCMapper组件就是WordCountMR中的   第一个阶段的  map 函数的定义之地
 * 
 * mapper组件有四个泛型：
 * 
 * 	mapper组件中的map方法的参数的类型， 
 * 
 *  前一对是  输入的 key -value的类型
 *  
 *  后一对是 输出的 key-value的类型
 * 
 * @param <KEYIN> the key input type to the Mapper
 * @param <VALUEIN> the value input type to the Mapper
 * @param <KEYOUT> the key output type from the Mapper
 * @param <VALUEOUT> the value output type from the Mapper
 * 
 * hadoop框架中的进行序列化的方式： 
 * java当中某个对象要进行序列化时对应的操作：  implements Serialisable  
 * 
 * java的原生序列化机制非常臃肿。除了我关心的这个对象中的属性的数据意外，还携带了包含该对象的所有其他必要信息，比如类的继承结构
 * 但是在hadoop的mapreduce编程当中，其实我们仅仅只需要关注 属性的值 就OK 
 *
 * 在hadoop的mapreduce编程中，我们必须使用一种新的序列化机制。
 * 在hadoop的这种序列化机制中，其实就是让我们的用户自定义类去实现 Writable 接口
 * 
 * 在hadoop的序列化框架中，已经为用户实现了好了基本类型和string类型的序列化。
 * 
 * int --- IntWritable
 * long ---- LongWritable 
 * .....
 * 
 * String ---- Text 
 */

/**
 *  如果在没有更改默认的数据读取组件的情况下， 那么mapper组件的输入key-value的类型必须是   LongWritable和Text
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	
	/**
	 *  key : 当前的value这一行在当前整个文件中的起始偏移量
	 * 
	 *  value :  就是默认的数据读取组件每次读取的一行数据
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
		/**
		 * 输出的key-value
		 * 
		 *    输出的  key-value的含义：  某个单词出现了几次
		 *    
		 *    某个特定的单词 出现了 一次
		 *    
		 *     key:  word
		 *     value : 1
		 */
		
		String[] split = value.toString().split(" ");
		
		for(String word :  split){
		
			context.write(new Text(word), new IntWritable(1));
		}
		
	}
}
