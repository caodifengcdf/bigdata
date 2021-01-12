package input.test.CombineFile;

import mr.test.sougoudata.DataCleanMain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class ComebineFileMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"CombineSmallFile");

        job.setJarByClass(ComebineFileMain.class);

        //设置job输入输出格式、map类、combin类、reduce类、map输出KV类型、reduce输出KV类型
        job.setInputFormatClass(CombineFileInput.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);//hdfs dfs -text查看，-cat有些地方显示？

        job.setMapperClass(CBFMapper.class);
        //job.setCombinerClass(WCReduce.class);
        job.setReducerClass(Reducer.class);//使用1个默认的Reducer，将多个map生成的文件合成一个

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //设置输入输出路径
        Path path1 = new Path(args[0]);
        Path path2 = new Path(args[1]); //输出为一个目录，结果统计在part-r-XXXX中，有几个reduce就有几个这种文件，默认为1个reduce
        //
        FileInputFormat.setInputPaths(job,path1);
        FileOutputFormat.setOutputPath(job,path2);

        //设置reduce个数
        job.setNumReduceTasks(1);
        //提交job
        job.waitForCompletion(true);

    }

    public static class CBFMapper extends Mapper<NullWritable, BytesWritable, Text,BytesWritable> {
        //K1:不需要，只需要读取整个文件内容
        //V1：文件内容
        //K2：文件名
        //V2：文件内容
        private Text key1;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //预先获取Key，即文件名
            InputSplit inputSplit = context.getInputSplit();
            Path path = ((FileSplit)inputSplit).getPath();
            String file_name = path.getName();
            key1 = new Text(file_name);

        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(key1, value);
        }
    }
}
