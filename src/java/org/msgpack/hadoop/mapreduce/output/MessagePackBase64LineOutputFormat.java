package org.msgpack.hadoop.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.msgpack.hadoop.mapreduce.io.MessagePackWritable;

public abstract class MessagePackBase64LineOutputFormat<M, W extends MessagePackWritable<M>> extends FileOutputFormat<NullWritable, W> {
    public RecordWriter<NullWritable, W> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        Path file = getDefaultWorkFile(job, "");
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        return new MessagePackBase64LineRecordWriter<M, W>(fileOut);
    }
}
