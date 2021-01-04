package mapreduce.test.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TestReduce extends Reducer <Text , IntWritable ,Text , IntWritable> {
    @Override
    //父类Reducer中reduce：context.write(key, value)
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values){
            sum += value.get();

        //数据已经经过merge归并，每组K,V类似['AAAA',[1,1,1,1,1,1]],故将V求和后输出即可
        context.write(key,new IntWritable(sum));

        }
    }
}
