package mapreduce.test.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TestMapper extends Mapper <LongWritable, Text , Text, IntWritable>{
    @Override
    //覆盖父类map方法，Mapper中run发放按行获得数据，再调用map方法输出K，V
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //每个单词分开，形成数组
        String[] words = line.split("\t");
        //父类map直接context.write(key, value);
        for (String word : words){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
