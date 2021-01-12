package output.test.TwoPathOutputFile;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //防止重复的进行了merge操作，<null,null,.....>,所以历遍

        for (NullWritable value:values){
            context.write(key,NullWritable.get());
        }
    }
}
