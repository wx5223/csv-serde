package com.shawn.hive.serde;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by shawn on 2018/2/28.
 */
public class CSVMLineRecordReader implements RecordReader<LongWritable, Text> {
    private static final Log LOG = LogFactory.getLog(LineRecordReader.class.getName());
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private CSVMLineRecordReader.LineReader in;
    int maxLineLength;
    private Seekable filePosition;
    private CompressionCodec codec;
    private Decompressor decompressor;

    public CSVMLineRecordReader(Configuration job, FileSplit split) throws IOException {
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", 2147483647);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        Path file = split.getPath();
        this.compressionCodecs = new CompressionCodecFactory(job);
        this.codec = this.compressionCodecs.getCodec(file);
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        if(this.isCompressedInput()) {
            this.decompressor = CodecPool.getDecompressor(this.codec);
            if(this.codec instanceof SplittableCompressionCodec) {
                SplitCompressionInputStream cIn = ((SplittableCompressionCodec)this.codec).createInputStream(fileIn, this.decompressor, this.start, this.end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
                this.in = new CSVMLineRecordReader.LineReader(cIn, job);
                this.start = cIn.getAdjustedStart();
                this.end = cIn.getAdjustedEnd();
                this.filePosition = cIn;
            } else {
                this.in = new CSVMLineRecordReader.LineReader(this.codec.createInputStream(fileIn, this.decompressor), job);
                this.filePosition = fileIn;
            }
        } else {
            fileIn.seek(this.start);
            this.in = new CSVMLineRecordReader.LineReader(fileIn, job);
            this.filePosition = fileIn;
        }

        if(this.start != 0L) {
            this.start += (long)this.in.readLine(new Text(), 0, this.maxBytesToConsume(this.start));
        }

        this.pos = this.start;
    }

    private boolean isCompressedInput() {
        return this.codec != null;
    }

    private int maxBytesToConsume(long pos) {
        return this.isCompressedInput()?2147483647:(int)Math.min(2147483647L, this.end - pos);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if(this.isCompressedInput() && null != this.filePosition) {
            retVal = this.filePosition.getPos();
        } else {
            retVal = this.pos;
        }

        return retVal;
    }

    public CSVMLineRecordReader(InputStream in, long offset, long endOffset, int maxLineLength) {
        this.maxLineLength = maxLineLength;
        this.in = new CSVMLineRecordReader.LineReader(in);
        this.start = offset;
        this.pos = offset;
        this.end = endOffset;
        this.filePosition = null;
    }

    public CSVMLineRecordReader(InputStream in, long offset, long endOffset, Configuration job) throws IOException {
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", 2147483647);
        this.in = new CSVMLineRecordReader.LineReader(in, job);
        this.start = offset;
        this.pos = offset;
        this.end = endOffset;
        this.filePosition = null;
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public Text createValue() {
        return new Text();
    }

    public synchronized boolean next(LongWritable key, Text value) throws IOException {
        while(this.getFilePosition() <= this.end) {
            key.set(this.pos);
            int newSize = this.in.readLine(value, this.maxLineLength, Math.max(this.maxBytesToConsume(this.pos), this.maxLineLength));

            if(newSize == 0) {
                return false;
            }
            String lines = value.toString();
            int nums = StringUtils.countMatches(lines, "\"");
            while (nums > 0 && nums % 2 != 0) {
                newSize += this.in.readLine(value, this.maxLineLength, Math.max(this.maxBytesToConsume(this.pos), this.maxLineLength));
                lines += value.toString();
                value.set(lines);
                nums = StringUtils.countMatches(lines, "\"");
                if(newSize > this.maxLineLength) {
                    break;
                }
            }
            this.pos += (long)newSize;
            if(newSize < this.maxLineLength) {
                return true;
            }

            LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - (long)newSize));
        }

        return false;
    }

    public float getProgress() throws IOException {
        return this.start == this.end?0.0F:Math.min(1.0F, (float)(this.getFilePosition() - this.start) / (float)(this.end - this.start));
    }

    public synchronized long getPos() throws IOException {
        return this.pos;
    }

    public synchronized void close() throws IOException {
        try {
            if(this.in != null) {
                this.in.close();
            }
        } finally {
            if(this.decompressor != null) {
                CodecPool.returnDecompressor(this.decompressor);
            }

        }

    }

    public static class LineReader extends org.apache.hadoop.util.LineReader {
        LineReader(InputStream in) {
            super(in);
        }

        LineReader(InputStream in, int bufferSize) {
            super(in, bufferSize);
        }

        public LineReader(InputStream in, Configuration conf) throws IOException {
            super(in, conf);
        }
    }
}