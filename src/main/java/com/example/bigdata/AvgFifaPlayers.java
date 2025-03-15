package com.example.bigdata;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class AvgFifaPlayers extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new AvgFifaPlayers(), args);
        System.exit(res);
    }

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "AvgFifaPlayers");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.setMapperClass(AvgFifaPlayersMapper.class);
        job.setCombinerClass(AvgFifaPlayersCombiner.class);
        job.setReducerClass(AvgFifaPlayersReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class AvgFifaPlayersMapper extends Mapper<LongWritable, Text, Text, SumCount> {

        private final Text leagueId = new Text();
        private final SumCount sumCount = new SumCount();

        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                if (offset.get() != 0) {
                    String line = lineText.toString();
                    int i = 0;
                    int weight = 0;
                    int wage = 0;
                    int age = 0;

                    for (String word : line
                        .split(";(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) {
                        if (i == 11) {
                            wage = Integer.parseInt(word);
                        }
                        if (i == 12) {
                            age = Integer.parseInt(word);
                        }
                        if (i == 15) {
                            weight = Integer.parseInt(word);
                        }
                        if (i == 16) {
                            leagueId.set(word);
                        }
                        i++;
                    }
                    if (weight < 100) {
                        sumCount.set(new DoubleWritable(wage), new DoubleWritable(age), new IntWritable(1));
                        context.write(leagueId, sumCount);
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class AvgFifaPlayersCombiner extends Reducer<Text, SumCount, Text, SumCount> {

        private final SumCount result = new SumCount();

        public void reduce(Text key, Iterable<SumCount> values, Context context) {
            try {
                double sumWage = 0;
                double sumAge = 0;
                int count = 0;
                for (SumCount value : values) {
                    sumWage += value.getWage().get();
                    sumAge += value.getAge().get();
                    count += value.getCount().get();
                }
                result.set(new DoubleWritable(sumWage), new DoubleWritable(sumAge), new IntWritable(count));
                context.write(key, result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class AvgFifaPlayersReducer extends Reducer<Text, SumCount, Text, Text> {

        private final Text resultValue = new Text();

        public void reduce(Text key, Iterable<SumCount> values, Context context) {
            try {
                double sumWage = 0;
                double sumAge = 0;
                int count = 0;
                for (SumCount value : values) {
                    sumWage += value.getWage().get();
                    sumAge += value.getAge().get();
                    count += value.getCount().get();
                }
                double averageWage = sumWage / count;
                double averageAge = sumAge / count;

                // Formatowanie wszystkich 3 wartości jako ciąg znaków
                resultValue.set(String.format("%.2f,%.2f,%d", averageWage, averageAge, count));
                context.write(key, resultValue);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
