package com.ikyxxs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<IntWritable,Text,Text,IntWritable> {

    private Text text = new Text();

    public void reduce(IntWritable key,Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text val : values) {
            text.set(val);
            context.write(text,key);
        }
    }
}