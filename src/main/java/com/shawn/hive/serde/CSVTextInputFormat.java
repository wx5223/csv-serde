package com.shawn.hive.serde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.shawn.hive.serde.CSVNLineInputFormat.DEFAULT_LINES_PER_MAP;
import static org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.LINES_PER_MAP;

/**
 * Created by shawn on 2018/2/12.
 */
public class CSVTextInputFormat extends FileInputFormat<LongWritable, Text> implements JobConfigurable{


    private CompressionCodecFactory compressionCodecs = null;

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        LOG.info("Initiated csv input format");

        reporter.setStatus(genericSplit.toString());
        // Check if we should throw away this split
        long start = ((FileSplit)genericSplit).getStart();
        Path file = ((FileSplit)genericSplit).getPath();
        final CompressionCodec codec = compressionCodecs.getCodec(file);
        if (codec != null && start != 0) {
            // (codec != null) means the file is not splittable.
            return new EmptyRecordReader();
        }


        return new CSVMLineRecordReader(job, (FileSplit) genericSplit);
    }

    public void configure(JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return compressionCodecs.getCodec(file) == null;
        //return false;
    }

    static class EmptyRecordReader implements RecordReader<LongWritable, Text> {
        @Override
        public void close() throws IOException {}
        @Override
        public LongWritable createKey() {
            return new LongWritable();
        }
        @Override
        public Text createValue() {
            return new Text();
        }
        @Override
        public long getPos() throws IOException {
            return 0;
        }
        @Override
        public float getProgress() throws IOException {
            return 0;
        }
        @Override
        public boolean next(LongWritable key, Text value) throws IOException {
            return false;
        }
    }

    /*

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        int numLinesPerSplit = getNumLinesPerSplit(job);
        for (FileStatus status : listStatus(job)) {
            List<FileSplit> fileSplits = getSplitsForFile(status, job, numLinesPerSplit);
            splits.addAll(fileSplits);
        }
        return splits.toArray(new InputSplit[]{});
    }

    public static int getNumLinesPerSplit(JobConf job) {
        return job.getInt(LINES_PER_MAP, DEFAULT_LINES_PER_MAP);
    }

    public static List<FileSplit> getSplitsForFile(FileStatus status, JobConf conf, int numLinesPerSplit)
            throws IOException {
        List<FileSplit> splits = new ArrayList<FileSplit>();
        Path fileName = status.getPath();
        if (status.isDir()) {
            throw new IOException("Not a file: " + fileName);
        }
        FileSystem fs = fileName.getFileSystem(conf);
        CSVLineRecordReader lr = null;
        try {
            FSDataInputStream in = fs.open(fileName);
            lr = new CSVLineRecordReader(in, conf);
            List<Text> line = new ArrayList<Text>();
            int numLines = 0;
            long begin = 0;
            long length = 0;
            int num = -1;
            while ((num = lr.readLine(line)) > 0) {
                numLines++;
                length += num;
                if (numLines == numLinesPerSplit) {
                    // To make sure that each mapper gets N lines,
                    // we move back the upper split limits of each split
                    // by one character here.
                    if (begin == 0) {
                        splits.add(new FileSplit(fileName, begin, length - 1, new String[] {}));
                    } else {
                        splits.add(new FileSplit(fileName, begin - 1, length, new String[] {}));
                    }
                    begin += length;
                    length = 0;
                    numLines = 0;
                }
            }
            if (numLines != 0) {
                splits.add(new FileSplit(fileName, begin, length, new String[] {}));
            }
        } finally {
            if (lr != null) {
                lr.close();
            }
        }
        return splits;
    }




*/



}