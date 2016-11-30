package com.ikyxxs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apdplat.word.dictionary.DictionaryFactory;
import org.apdplat.word.util.WordConfTools;

import java.io.IOException;

public class WordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //添加自定义词典
//        WordConfTools.set("dic.path", "classpath:dic.txt");
//        DictionaryFactory.reload();

        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage:wordcount<in><out>");
            System.exit(2);
        }

        Path inputDir = new Path(args[0]);
        Path tempDir = new Path("temp");
        Path outputDir = new Path(args[1]);

        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setCombinerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, tempDir);
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);

        if(job.waitForCompletion(true)) {
            Job sortJob = new Job(conf, "result sort");
            sortJob.setJarByClass(WordCount.class);

            FileInputFormat.addInputPath(sortJob, tempDir);

            sortJob.setMapperClass(SortMapper.class);
            sortJob.setReducerClass(SortReducer.class);
            FileOutputFormat.setOutputPath(sortJob, outputDir);

            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);

            sortJob.setSortComparatorClass(SortComparator.class);

            FileSystem.get(conf).deleteOnExit(tempDir);

            System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}