package org.msgpack.hadoop.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

import org.msgpack.MessagePack;

/**
 * A Hadoop Writable wrapper for MessagePack of type C.
 */
    
public class MessagePackWritable<C> implements WritableComparable<MessagePackWritable<C>> {
    private C obj_ = null;

    public MessagePackWritable(C obj) {
        // TODO: 2010/11/09 Kazuki Ohta <kazuki.ohta@gmail.com>
        // Make all fields as nullable.
        MessagePack.register(obj.getClass());

        obj_ = obj;
    }

    public void set(C obj) { obj_ = obj; }

    public C get() { return obj_; }
    
    public void write(DataOutput out) throws IOException {
        byte[] raw = MessagePack.pack(obj_);
        if (raw == null) return;
        out.writeInt(raw.length);
        out.write(raw, 0, raw.length);
    }

    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        if (size > 0) {
            byte[] raw = new byte[size];
            in.readFully(raw, 0, size);

            // TODO: 2010/11/09 Kazuki Ohta <kazuki.ohta@gmail.com>
            // want to supress allocation here by recycling obj_
            obj_ = (C)MessagePack.unpack(raw, obj_.getClass());
        }
    }

    @Override
    public int compareTo(MessagePackWritable<C> other) {
        // TODO: 2010/11/09 Kazuki Ohta <kazuki.ohta@gmail.com>
        // compare without packing
        byte[] raw1 = MessagePack.pack(this.get());
        byte[] raw2 = MessagePack.pack(other.get());
        return BytesWritable.Comparator.compareBytes(raw1, 0, raw1.length, raw2, 0, raw2.length);
    }
}
