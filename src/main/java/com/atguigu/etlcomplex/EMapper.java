package com.atguigu.etlcomplex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private LogBean log = new LogBean();
    private Text k = new Text();
    private Counter pass;
    private Counter fail;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pass = context.getCounter("ETL", "Pass");
        fail = context.getCounter("ETL", "Fail");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        parseLog(line);

        if (log.isValid()) {
            k.set(log.toString());
            context.write(k, NullWritable.get());
            pass.increment(1);
        } else {
            fail.increment(1);
        }
    }

    private void parseLog(String line) {
        // 1 截取
        String[] fields = line.split(" ");

        if (fields.length > 11) {

            // 2封装数据
            log.setRemote_addr(fields[0]);
            log.setRemote_user(fields[1]);
            log.setTime_local(fields[3].substring(1));
            log.setRequest(fields[6]);
            log.setStatus(fields[8]);
            log.setBody_bytes_sent(fields[9]);
            log.setHttp_referer(fields[10]);

            if (fields.length > 12) {
                log.setHttp_user_agent(fields[11] + " "+ fields[12]);
            }else {
                log.setHttp_user_agent(fields[11]);
            }

            // 大于400，HTTP错误
            if (Integer.parseInt(log.getStatus()) >= 400) {
                log.setValid(false);
            } else {
                log.setValid(true);
            }
        }else {
            log.setValid(false);
        }

    }
}
