package mr.test.sougoudata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DataCleanMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2 || args == null) {
            System.out.println("pleast input two path");
            System.exit(0);
        }
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"sougoudataclean");

        //设置job的jar包，一般设置为主类，用来判断当运行的jar包中的类名是否与这参数一致
        job.setJarByClass(DataCleanMain.class);
        //设置job输入输出格式、map类、combin类、reduce类、map输出KV类型、reduce输出KV类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(DCMapper.class);
        //job.setCombinerClass(WCReduce.class);//无需合并，没有reduce
        //job.setReducerClass(WCReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);//将行数据放入Key中，不需要设置Value

        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(LongWritable.class);

        //设置输入输出路径
        Path path1 = new Path(args[0]);
        Path path2 = new Path(args[1]); //输出为一个目录，结果统计在part-r-XXXX中，有几个reduce就有几个这种文件，默认为1个reduce
        //
        FileInputFormat.setInputPaths(job,path1);
        FileOutputFormat.setOutputPath(job,path2);

        //设置reduce个数
        job.setNumReduceTasks(0);
        //提交job
        job.waitForCompletion(true);

    }


    //DataCleanMapper:在main中调用需要使用静态方法，用途和python中的方法一样，无需创建对象。
    public static class DCMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    NullWritable nullWritable = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Counter counter = context.getCounter("name1", "name2");//1计数器的组名，2计数器的名称

            String[] data = value.toString().split("\t");
            if (data.length == 6) {
                context.write(value, nullWritable);
            } else {
                counter.increment(1);
            }

        }
    }
}
//结果 name2=6,6条数据错误