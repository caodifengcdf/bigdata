package mr.test.sougoudata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DataUserCountMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2 || args == null) {
            System.out.println("pleast input two path");
            System.exit(0);
        }
        Configuration configuration = new Configuration();

        //启用压缩
        configuration.set("mapreduce.map.output.compress", "true");
        //设置map输出的压缩算法是：BZip2Codec，它是hadoop默认支持的压缩算法，且支持切分
        configuration.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        //开启job输出压缩功能
        configuration.set("mapreduce.output.fileoutputformat.compress", "true");
        //指定job输出使用的压缩算法
        configuration.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");

        //生成job实例
        Job job = Job.getInstance(configuration,"sougoudataclean");

        //设置job的jar包，一般设置为主类，用来判断当运行的jar包中的类名是否与这参数一致
        job.setJarByClass(DataUserCountMain.class);
        //设置job输入输出格式、map类、combin类、reduce类、map输出KV类型、reduce输出KV类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入输出路径
        //hdfs://192.168.61.101:9000/hadoop_test/sougou_usercount/part-m-00000 hdfs://192.168.61.101:9000/hadoop_test/sougou_usercount1
        Path path1 = new Path(args[0]);
        Path path2 = new Path(args[1]); //输出为一个目录，结果统计在part-r-XXXX中，有几个reduce就有几个这种文件，默认为1个reduce
        //
        FileInputFormat.setInputPaths(job,path1);
        FileOutputFormat.setOutputPath(job,path2);

        //设置reduce个数,默认为1
        job.setNumReduceTasks(1);
        //提交job
        job.waitForCompletion(true);

    }

    public static class MyMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        Text user_id = new Text();
        IntWritable count = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String userid = value.toString().split("\t")[0];
            user_id.set(userid);
            context.write(user_id,count);
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable,Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value:values){
                sum += value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
}
//hdfs dfs -cat /hadoop_test/sougou_usercount1/part-r-00000 | head
//查看前10条数据
//20111230111308	10
//20111230111309	10
//20111230111310	17
//20111230111311	16
//20111230111312	15
//20111230111313	6
//20111230111314	9
//20111230111315	8
//20111230111316	19
//20111230111317	13
