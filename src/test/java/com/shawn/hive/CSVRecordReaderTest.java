package com.shawn.hive;

import com.shawn.hive.serde.CSVMLineRecordReader;
import com.shawn.hive.serde.CSVRecordReader;
import com.shawn.hive.serde.CSVTextInputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shawn on 2018/2/24.
 */
public class CSVRecordReaderTest {
    @Test
    public void testRead() throws IOException {
        //32449658
        final Path file = new Path("file:///Users/shawn/Documents/test0001.csv");
        FileSystem fs = FileSystem.getLocal(createConfig());
        FSDataInputStream fileIn = fs.open(file);
        fileIn.seek(32447672l);
        Reader in = new BufferedReader(new InputStreamReader(fileIn));
        int i = 0;
        char c;
        StringBuffer sb = new StringBuffer();
        int line = 1;
        while ((i = in.read()) != -1) {
            c = (char) i;
            sb.append(c);
            if (c == '\n') {
                System.out.println(sb.toString());
                if (line > 10) {
                    break;
                }
                line++;
            }
        }
    }

    @Test
    public void testReadLine() throws Exception {
        JobConf conf = createConfig();
        CSVTextInputFormat inputFormat = new CSVTextInputFormat();
        inputFormat.configure(conf);
        InputSplit[] actualSplits = inputFormat.getSplits(conf, 10);
        LongWritable l = new LongWritable();
        Text t = new Text();
        int i =0;
        for (InputSplit actualSplit : actualSplits) {
            RecordReader<LongWritable, Text> recordReader = new CSVMLineRecordReader(conf, (FileSplit) actualSplit);/*inputFormat.getRecordReader(actualSplit, conf, new Reporter() {
                @Override
                public void setStatus(String s) {

                }

                @Override
                public Counters.Counter getCounter(Enum<?> anEnum) {
                    return null;
                }

                @Override
                public Counters.Counter getCounter(String s, String s1) {
                    return null;
                }

                @Override
                public void incrCounter(Enum<?> anEnum, long l) {

                }

                @Override
                public void incrCounter(String s, String s1, long l) {

                }

                @Override
                public InputSplit getInputSplit() throws UnsupportedOperationException {
                    return null;
                }

                @Override
                public float getProgress() {
                    return 0;
                }

                @Override
                public void progress() {

                }
            });*/
            List<String> cacheValue = new ArrayList<String>();
            while(recordReader.next(l, t)){
                /*if (cacheValue.contains(t.toString())) {
                    System.out.println("duplicated");
                }
                cacheValue.add(t.toString());*/
                i++;
            };
            System.out.println(i);
            //System.out.printf(""+l.toString()+t.toString());
        }


    }

    private JobConf createConfig() {
        JobConf conf = new JobConf();

        //conf.setStrings("mapreduce.input.fileinputformat.inputdir", "/home/shawn/Document/test1.csv");
        conf.setStrings("mapred.input.dir", "/Users/shawn/Documents/test0001.csv");
        //conf.setStrings("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec");
        conf.setStrings("columns", "_id,opertime,operip,provinceid,cityid,nettype,static,firstvisittime,sourcepagevisittime,pagevisittime,userid,usertype,sourceurl,sourcetitle,pageurl,pagetitle,sessionid,entertime,leavetime,sourceurlpattern,sourcepagek1,sourcepagek2,sourcepagek3,sourcepagek4,sourcepagek5,pageurlpattern,pagek1,pagek2,pagek3,pagek4,statuscode,system,browser,resolution,screencolor,flashsupport,javasupport,clickid,visitid,coordinate,datatype,devicetype,channelid,clientid,appname,storetime,useraccount,platform,behaviorid,proprice,reservedid,BLOC");

        return conf;
    }
}
