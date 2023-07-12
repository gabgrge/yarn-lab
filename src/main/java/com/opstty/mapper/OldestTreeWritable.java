package com.opstty.mapper;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OldestTreeWritable implements Writable {
    private IntWritable year;
    private IntWritable district;

    public OldestTreeWritable() {
        this.year = new IntWritable();
        this.district = new IntWritable();
    }

    public OldestTreeWritable(int year, int district) {
        this.year = new IntWritable(year);
        this.district = new IntWritable(district);
    }

    public int getYear() {
        return year.get();
    }

    public int getDistrict() {
        return district.get();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        year.write(dataOutput);
        district.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year.readFields(dataInput);
        district.readFields(dataInput);
    }

    @Override
    public String toString() {
        return year.toString() + "\t" + district.toString();
    }
}

