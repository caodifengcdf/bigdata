package mr.test.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WCMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        //使用默认参数
        Configuration configuration = new Configuration();
        //生成job对象 需要传入配置参数及job名
        Job job = Job.getInstance(configuration,"wordcount");

        //设置job的jar包，一般设置为主类，用来判断当运行的jar包中的类名是否与这参数一致
        job.setJarByClass(WCMain.class);
        //设置job输入输出格式、map类、combin类、reduce类、map输出KV类型、reduce输出KV类型
        job.setInputFormatClass(TextInputFormat.class); //可以不设置，默认就是TextInputFormat.class
        job.setOutputFormatClass(TextOutputFormat.class);//可以不设置，默认就是TextOutputFormat.class

        job.setMapperClass(WCMapper.class);
        job.setCombinerClass(WCReduce.class);//设置此类后，会在map输出前进行combine操作，现将一个block中的数据词汇进行统计
        job.setReducerClass(WCReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class); //没有“reduce”，reduce的输出就是整个job的输出
        job.setOutputValueClass(LongWritable.class);//没有”reduce“，reduce的输出就是整个job的输出

        //设置输入输出路径
        Path path1 = new Path(args[0]);
        Path path2 = new Path(args[1]); //输出为一个目录，结果统计在part-r-XXXX中，有几个reduce就有几个这种文件，默认为1个reduce
        //
        FileInputFormat.setInputPaths(job,path1);
        FileOutputFormat.setOutputPath(job,path2);
        //提交job
        job.waitForCompletion(true);


    }
}
