package edu.itu.hadoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyWritable implements Writable,
        WritableComparable<KeyWritable> {

    private String primaryKey;
    private String secondaryKey;
    private String delimiter;

    public KeyWritable() {}

    public KeyWritable(String primaryKey, String secondaryKey) {
        this.primaryKey = primaryKey;
        this.secondaryKey = secondaryKey;
    }

    public KeyWritable(String primaryKey, String secondaryKey, String delimiter) {
        this.primaryKey = primaryKey;
        this.secondaryKey = secondaryKey;
        this.delimiter = delimiter;
    }

    public String getPrimaryKey() {
        return this.primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getSecondaryKey() {
        return this.secondaryKey;
    }

    public void setSecondaryKey(String secondaryKey) {
        this.secondaryKey = secondaryKey;
    }

    public String getDelimiter() {
        return this.delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public String toString() {
        return (this.primaryKey + this.delimiter + this.secondaryKey);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.primaryKey = WritableUtils.readString(dataInput);
        this.delimiter = WritableUtils.readString(dataInput);
        this.secondaryKey = WritableUtils.readString(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, this.primaryKey);
        WritableUtils.writeString(dataOutput, this.delimiter);
        WritableUtils.writeString(dataOutput, this.secondaryKey);
    }

    @Override
    public int compareTo(KeyWritable compositeKey) {
        int result = this.primaryKey.compareTo(compositeKey.getPrimaryKey());
        if (result == 0) {
            result = this.secondaryKey.compareTo(compositeKey.getSecondaryKey());
        }
        return result;
    }
}
