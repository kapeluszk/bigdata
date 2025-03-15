package com.example.bigdata;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;


public class SumCount implements WritableComparable<SumCount> {

    DoubleWritable wage;
    DoubleWritable age;
    IntWritable count;

    public SumCount() {
        set(new DoubleWritable(0), new DoubleWritable(0), new IntWritable(0));
    }

    public SumCount(Double wage, Double age, Integer count) {
        set(new DoubleWritable(wage), new DoubleWritable(age), new IntWritable(count));
    }

    public void set(DoubleWritable wage, DoubleWritable age, IntWritable count) {
        this.wage = wage;
        this.age = age;
        this.count = count;
    }

    public DoubleWritable getWage() {
        return wage;
    }

    public DoubleWritable getAge() {
        return age;
    }

    public IntWritable getCount() {
        return count;
    }

    public void addSumCount(SumCount sumCount) {
        set(new DoubleWritable(this.wage.get() + sumCount.getWage().get()), new DoubleWritable(this.age.get() + sumCount.getAge().get()), new IntWritable(this.count.get() + sumCount.getCount().get()));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        wage.write(dataOutput);
        age.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        wage.readFields(dataInput);
        age.readFields(dataInput);
        count.readFields(dataInput);
    }

    @Override
    public int compareTo(SumCount sumCount) {

        // compares the first of the two values
        int comparison = wage.compareTo(sumCount.wage);
        int comparison2 = age.compareTo(sumCount.age);

        // if they're not equal, return the value of compareTo between the "sum" value
        if (comparison != 0) {
            return comparison;
        }
        if (comparison2 != 0) {
            return comparison2;
        }

        // if they're equal, return the value of compareTo between the "count" value
        return count.compareTo(sumCount.count);
    }

}
