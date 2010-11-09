package org.msgpack.hadoop.mapreduce.input;

import java.io.*;
import java.util.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;

import org.msgpack.MessagePack;
import org.msgpack.hadoop.mapreduce.io.MessagePackWritable;

import junit.framework.TestCase;
import static org.junit.Assert.assertEquals;

/**
 * Test cases from MessagePackBase64LineInputFormat.
 * @see TestLineInputFormat.java
 */
public class TestMessagePackBase64LineInputFormat extends TestCase {
    public static class MyClass {
        public String s;
        public int v;
    }

    static {
        MessagePack.register(MyClass.class);
    }

    public static class MyClassWritable extends MessagePackWritable<MyClass> {
        public MyClassWritable() {
            super(new MyClass());
        }
    }

    public static class MyClassMessagePackBase64LineInputFormat extends MessagePackBase64LineInputFormat<MyClass, MyClassWritable> {
        public MyClassMessagePackBase64LineInputFormat() {
            setMessagePackWritable(new MyClassWritable());
        }
    }

    //--------------

    private static int MAX_LENGTH = 200;
    private final Base64 base64_ = new Base64();
    private static Configuration defaultConf = new Configuration();
    private static FileSystem localFs = null; 

    private static Path workDir = 
        new Path(new Path(System.getProperty("test.build.data", "."), "data"),
                 "TestLineInputFormat");

    public void testFormat() throws Exception {
        localFs = FileSystem.getLocal(defaultConf);
        localFs.delete(workDir, true);

        Job job = new Job(new Configuration(defaultConf));
        Path file = new Path(workDir, "test.txt");

        int seed = new Random().nextInt();
        Random random = new Random(seed);

        // for a variety of lengths
        for (int length = 0; length < MAX_LENGTH;
             length += random.nextInt(MAX_LENGTH/10) + 1) {
            // create a file with length entries
            Writer writer = new OutputStreamWriter(localFs.create(file));
            try {
                MyClass mc = new MyClass();
                for (int i = 0; i < length; i++) {
                    mc.s = Integer.toString(i);
                    mc.v = i;
                    byte[] raw = MessagePack.pack(mc);
                    byte[] b64e = base64_.encodeBase64(raw);
                    byte[] b64d = base64_.decode(b64e);
                    MyClass mc2 = MessagePack.unpack(b64d, mc.getClass());
                    assertEquals(mc.s, mc2.s);
                    assertEquals(mc.v, mc2.v);

                    writer.write(base64_.encodeToString(raw));
                }
            } finally {
                writer.close();
            }
            checkFormat(job);
        }
    }

    void checkFormat(Job job) throws Exception {
        TaskAttemptContext attemptContext = new TaskAttemptContext(job.getConfiguration(),
                                                                   new TaskAttemptID("123", 0, false, 1, 2));

        MyClassMessagePackBase64LineInputFormat format = new MyClassMessagePackBase64LineInputFormat();
        FileInputFormat.setInputPaths(job, workDir);

        List<InputSplit> splits = format.getSplits(job);
        for (int j = 0; j < splits.size(); j++) {
            RecordReader<LongWritable, MyClassWritable> reader =
                format.createRecordReader(splits.get(j), attemptContext);
            reader.initialize(splits.get(j), attemptContext);

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
