package org.msgpack.hadoop.mapreduce.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.msgpack.hadoop.mapreduce.io.MessagePackWritable;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestMessagePackWritable {
    public static class MyClass {
        public String s;
        public int v;
    }

    public static class MyClassWritable extends MessagePackWritable<MyClass> {
        public MyClassWritable() {
            super(new MyClass());
        }
        public MyClassWritable(MyClass obj) {
            super(obj);
        }
    }

    @Test
    public void testReadWrite() throws Exception {
        MyClass b = new MyClass();
        b.s = "aiueo";
        b.v = 3;
        MyClassWritable bw = new MyClassWritable(b);
        DataOutputStream dos = new DataOutputStream(new FileOutputStream("test.txt"));
        bw.write(dos);
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream("test.txt"));
        MyClassWritable aw = new MyClassWritable();
        aw.readFields(dis);
        dis.close();

        assertEquals(b.s, aw.get().s);
        assertEquals(b.v, aw.get().v);
    }
}
