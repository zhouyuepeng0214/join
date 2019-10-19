package com.atguigu.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pMap = new HashMap<String, String>();
    private Text line = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fileSystem.open(new Path(cacheFiles[0]));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        String line;

        while (StringUtils.isNotEmpty(line = br.readLine())) {
            String[] fields = line.split("\t");
            pMap.put(fields[0], fields[1]);
        }
        IOUtils.closeStream(br);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        line.set(fields[0] + "\t" + pMap.get(fields[1]) + "\t" + fields[2]);
        context.write(line,NullWritable.get());
    }
}
