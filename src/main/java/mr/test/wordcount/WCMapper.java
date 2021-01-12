package mr.test.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapper extends Mapper <LongWritable, Text,Text,LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //key:一行文本偏移量
        //value:一行文本数据，默认调用的是TextFileInputFormat,按行读取。多行读取需要采用NLineIputFormat

        //将value转为字符再一个个按分隔符拆分
        String line = value.toString();
        String[] words = line.split(" ");
        //将K,V写入上下文中
        Text text = new Text();
        LongWritable longWritable = new LongWritable();

        for (String word : words){
            //需要将可序列化的K,V写入
            text.set(word);
            longWritable.set(1);
            context.write(text,longWritable);
        }
    }
}
