package org.msgpack.hadoop.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.codec.binary.Base64;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.msgpack.MessagePack;
import org.msgpack.hadoop.mapreduce.io.MessagePackWritable;

public class MessagePackBase64LineRecordReader<M, V extends MessagePackWritable<M>> extends RecordReader<LongWritable, V> {
    private static final Logger LOG = LoggerFactory.getLogger(MessagePackBase64LineRecordReader.class);

    private LineReader lineReader_;
    private final Text line_ = new Text();

    private final LongWritable key_ = new LongWritable(0);
    private final V val_;
    private final Base64 base64_ = new Base64();

    protected long start_;
    protected long pos_;
    protected long end_;
    private FSDataInputStream fileIn_;

    public MessagePackBase64LineRecordReader(V messagePackWritable) {
        val_ = messagePackWritable;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)genericSplit;
        start_ = split.getStart();
        end_ = start_ + split.getLength();
        final Path file = split.getPath();
        Configuration conf = context.getConfiguration();

        // Open the file
        FileSystem fs = file.getFileSystem(conf);
        fileIn_ = fs.open(split.getPath());

        // Creates input stream and also reads the file header
        lineReader_ = new LineReader(fileIn_, conf);

        // SSeek to the start of the split
        LOG.info("Seeking to split start at pos " + start_);
        if (start_ != 0) {
            fileIn_.seek(start_);
            skipToNextSyncPoint();
            start_ = fileIn_.getPos();
            LOG.info("Start is now " + start_);
        }
        pos_ = start_;
    }

    protected void skipToNextSyncPoint() throws IOException {
        lineReader_.readLine(new Text());
    }

    @Override
    public float getProgress() {
        if (start_ == end_) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos_ - start_) / (float) (end_ - start_));
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (lineReader_ != null) {
            lineReader_.close();
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key_;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return val_;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (pos_ <= end_) {
            int newSize = lineReader_.readLine(line_);
            if (newSize == 0) return false;

            pos_ = fileIn_.getPos();
            byte[] lineBytes = line_.toString().getBytes("UTF-8");

            long key = pos_;
            M val = val_.get();
            MessagePack.unpack(base64_.decode(lineBytes), val);
            if (val == null) continue;

            key_.set(key);
            val_.set(val);
            return true;
        }
        return false;
    }
}
