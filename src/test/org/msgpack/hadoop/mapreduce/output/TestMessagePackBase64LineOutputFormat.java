package org.msgpack.hadoop.mapreduce.output;

import java.io.*;
import java.util.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;

import org.msgpack.MessagePack;
import org.msgpack.hadoop.mapreduce.io.*;
import org.msgpack.hadoop.mapreduce.input.*;

import junit.framework.TestCase;
import org.apache.commons.logging.*;
import static org.junit.Assert.assertEquals;

/**
 * Test cases from MessagePackBase64LineOutputFormat.
 */

public class TestMessagePackBase64LineOutputFormat extends TestCase {
    private static final Log LOG =
        LogFactory.getLog(TestMessagePackBase64LineOutputFormat.class.getName());

    //--------
    public static class MyClass {
        public String s;
        public int v;
    }

    public static class MyClassWritable extends MessagePackWritable<MyClass> {
        protected MyClass getObjectInstance() { return new MyClass(); }
    }

    public static class MyClassMessagePackBase64LineInputFormat extends MessagePackBase64LineInputFormat<MyClass, MyClassWritable> {
        protected MyClassWritable getWritableInstance() { return new MyClassWritable(); }
    }

    public static class MyClassMessagePackBase64LineOutputFormat extends MessagePackBase64LineOutputFormat<MyClass, MyClassWritable> {}
    //--------

    public void testWrite() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf);

        Path outdir = new Path(System.getProperty("test.build.data", "/tmp"),
                               "outseq");
        FileOutputFormat.setOutputPath(job, outdir);

        FileSystem localFs = FileSystem.getLocal(conf);
        localFs.delete(outdir, true);

        // Output
        TaskAttemptContext context =
            new TaskAttemptContext(job.getConfiguration(),
                                   new TaskAttemptID("123", 0, false, 1, 2));
        OutputFormat<NullWritable, MyClassWritable> outputFormat =
            new MyClassMessagePackBase64LineOutputFormat();
        OutputCommitter committer = outputFormat.getOutputCommitter(context);
        committer.setupJob(job);
        RecordWriter<NullWritable, MyClassWritable> writer = outputFormat.
            getRecordWriter(context);

        LOG.info("Creating data by MyClassMessagePackBase64LineOutputFormat");
        try {
            MyClassWritable val = new MyClassWritable();
            for (int i = 0; i < 1000; ++i) {
                MyClass mc = val.get();
                mc.s = Integer.toString(i);
                mc.v = i;
                val.set(mc);
                writer.write(NullWritable.get(), val);
            }
        } finally {
            writer.close(context);
        }
        committer.commitTask(context);

        // Input
        InputFormat<LongWritable, MyClassWritable> inputFormat
            = new MyClassMessagePackBase64LineInputFormat();
        FileInputFormat.setInputPaths(job, outdir);
        List<InputSplit> splits = inputFormat.getSplits(job);
        for (int j = 0; j < splits.size(); j++) {
            RecordReader<LongWritable, MyClassWritable> reader =
                inputFormat.createRecordReader(splits.get(j), context);
            reader.initialize(splits.get(j), context);

            int count = 0;
            try {
                while (reader.nextKeyValue()) {
                    LongWritable key = reader.getCurrentKey();
                    MyClassWritable val = reader.getCurrentValue();
                    MyClass mc = val.get();
                    assertEquals(mc.v, count);
                    assertEquals(mc.s, Integer.toString(count));
                    count++;
                }
            } finally {
                reader.close();
            }
        }
    }
}
