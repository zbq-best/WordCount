package com.ikyxxs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

import java.io.IOException;
import java.util.List;

public class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text text = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        List<Word> wordList = WordSegmenter.seg(value.toString());      //分词（不包含停用词）

        if (null != wordList && !wordList.isEmpty()) {
            for (Word word : wordList) {
                text.set(word.getText());
                context.write(text, one);
            }
        }
    }
}