package org.msgpack.hadoop.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.msgpack.MessagePack;
import org.msgpack.hadoop.mapreduce.io.MessagePackWritable;

class MessagePackBase64LineRecordWriter<M, W extends MessagePackWritable<M>> extends RecordWriter<NullWritable, W> {
    protected final Base64 base64_;
    protected final DataOutputStream out_;
    
    public MessagePackBase64LineRecordWriter(DataOutputStream out) {
        base64_ = new Base64();
        out_ = out;
    }

    public void write(NullWritable key, W val) throws IOException, InterruptedException {
        M obj = val.get();
        assert(obj != null);
        byte[] raw = MessagePack.pack(obj);
        byte[] b64Bytes = base64_.encode(raw);
        out_.write(b64Bytes);
    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        out_.close();
    }
}
