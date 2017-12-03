package edu.itu.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {
    public KeyComparator() {
        super(KeyWritable.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        KeyWritable key1 = (KeyWritable) w1;
        KeyWritable key2 = (KeyWritable) w2;
        int result = key1.getPrimaryKey().compareTo(key2.getPrimaryKey());
        if (result == 0) {
            return key1.getSecondaryKey().compareTo(key2.getSecondaryKey());
        }
        return result;
    }
}
