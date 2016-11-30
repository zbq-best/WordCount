package com.ikyxxs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, IntWritable,Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        IntWritable times = new IntWritable(1);
        Text text = new Text();
        String eachLine = value.toString();
        String[] eachTerm = eachLine.split("\t");
        text.set(eachTerm[0]);
        times.set(Integer.parseInt(eachTerm[1]));
        context.write(times, text);
    }
}