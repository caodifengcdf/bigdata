package mr.test.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReduce extends Reducer <Text, LongWritable,Text, LongWritable> {
    @Override
    //shuffle阶段，将相同的KeyIn的merge到一起，例如[hello,Iterable(1,1,1)],表示将3个hello归到一起
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //将values求和后，K,求和的值写入context
        int sum = 0;
        for (LongWritable value : values){
            sum += value.get();
        }
        context.write(key,new LongWritable(sum));
        
    }
}
