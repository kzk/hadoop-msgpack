package org.msgpack.hadoop.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.msgpack.MessagePack;
import org.msgpack.hadoop.mapreduce.io.MessagePackWritable;

import org.junit.Test;
import junit.framework.TestCase;
import static org.junit.Assert.assertEquals;

public class TestMapReduce extends TestCase {
    private static FileSystem fs;

    static {
        try {
            fs = FileSystem.getLocal(new Configuration());
        } catch (IOException ioe) {
            fs = null;
        }
    }

    //--------
    public static class MyClass {
        public String s;
        public int v;
    }

    public static class MyClassWritable extends MessagePackWritable<MyClass> {
        protected MyClass getObjectInstance() { return new MyClass(); }
    }
    //--------

    static class RandomGenMapper
        extends Mapper<IntWritable, IntWritable, IntWritable, MyClassWritable> {
        MyClassWritable reduceVal = new MyClassWritable();
        public void map(IntWritable key, IntWritable val,
                        Context context) throws IOException, InterruptedException {
            MyClass mc = reduceVal.get();
            mc.s = new String("kzk");
            mc.v = key.get();
            reduceVal.set(mc);
            context.write(key, reduceVal);
        }
    }

    static class RandomGenReducer
        extends Reducer<IntWritable, MyClassWritable, IntWritable, MyClassWritable> {
        public void reduce(IntWritable key, Iterable<MyClassWritable> it,
                           Context context) throws IOException, InterruptedException {
            for (MyClassWritable iw : it)
                context.write(key, iw);
        }
    }

    @Test
    public void testMapReduce() throws Exception {
        Configuration conf = new Configuration();

        // generate input data
        Path testdir = new Path("mapred.loadtest");
        if (!fs.mkdirs(testdir)) {
            throw new IOException("Mkdirs failed to create " + testdir.toString());
        }

        Path randomIns = new Path(testdir, "genins");
        if (!fs.mkdirs(randomIns)) {
            throw new IOException("Mkdirs failed to create " + randomIns.toString());
        }

        Path answerkey = new Path(randomIns, "answer.key");
        SequenceFile.Writer out =
            SequenceFile.createWriter(fs, conf, answerkey, IntWritable.class,
                                      IntWritable.class,
                                      SequenceFile.CompressionType.NONE);
        try {
            for (int i = 0; i < 10; i++)
                out.append(new IntWritable(i), new IntWritable(i));
        } finally {
            out.close();
        }

        // prepare output directory
        Path randomOuts = new Path(testdir, "genouts");
        fs.delete(randomOuts, true);

        // run mapreduce
        Job genJob = new Job(conf);
        FileInputFormat.setInputPaths(genJob, randomIns);
        genJob.setInputFormatClass(SequenceFileInputFormat.class);
        genJob.setMapperClass(RandomGenMapper.class);
        FileOutputFormat.setOutputPath(genJob, randomOuts);
        genJob.setMapOutputKeyClass(IntWritable.class);
        genJob.setMapOutputValueClass(MyClassWritable.class);
        genJob.setOutputKeyClass(IntWritable.class);
        genJob.setOutputValueClass(MyClassWritable.class);
        genJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        genJob.setReducerClass(RandomGenReducer.class);
        genJob.setNumReduceTasks(1);
        genJob.waitForCompletion(true);

        // check the result
        Path recomputedkey = new Path(randomOuts, "part-r-00000");
        SequenceFile.Reader in = new SequenceFile.Reader(fs, recomputedkey, conf);
        try {
            IntWritable key = new IntWritable(100);
            MyClassWritable val = new MyClassWritable();
            for (int i = 0; i < 10; i++) {
                assertTrue("read must success", in.next(key, val));
                assertEquals(key.get(), i);
                assertEquals(key.get(), val.get().v);
                assertEquals(val.get().s, "kzk");
            }
        } finally {
            in.close();
        }
    }
}
