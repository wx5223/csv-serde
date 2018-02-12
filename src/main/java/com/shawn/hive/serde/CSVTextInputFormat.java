package com.shawn.hive.serde;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

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


        return new CSVRecordReader(job, (FileSplit) genericSplit);
    }

    public void configure(JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
        return compressionCodecs.getCodec(file) == null;
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
}