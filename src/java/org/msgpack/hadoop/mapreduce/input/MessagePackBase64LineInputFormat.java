package org.msgpack.hadoop.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.msgpack.hadoop.mapreduce.io.MessagePackWritable;

public abstract class MessagePackBase64LineInputFormat<M, V extends MessagePackWritable<M>> extends FileInputFormat<LongWritable, V> {
    private V writable_;

    protected void setMessagePackWritable(V writable) {
        writable_ = writable;
    }
    
    @Override
    public RecordReader<LongWritable, V> createRecordReader(InputSplit split,
        TaskAttemptContext taskAttempt) throws IOException, InterruptedException {
        return new MessagePackBase64LineRecordReader<M, V>(writable_);
    }
}
