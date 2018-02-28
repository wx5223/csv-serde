package com.shawn.hive.serde;

import com.opencsv.CSVParser;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipInputStream;


/**
 * Created by shawn on 2018/2/12.
 */
public class CSVRecordReader implements RecordReader<LongWritable, Text> {
    public static final String LINE_DELIMITER = "csvinputformat.record.delimiter";
    public static final String DEFAULT_DELIMITER = "\n";
    public static final String DEFAULT_QUOTE = "\"";
    public static final String DEFAULT_ESCAPE = "\\";
    private static final Log LOG = LogFactory.getLog(CSVRecordReader.class.getName());
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private int maxLineLength = 10000;
    protected Reader in;
    private String delimiter;
    private InputStream is;
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private int columnNum = 0;
    private CSVParser csvParser = new CSVParser(',', '"');

    /**
     * Default constructor is needed when called by reflection from hadoop
     */
    public CSVRecordReader() {
    }

    public CSVRecordReader(Configuration job, FileSplit split) throws IOException {
        LOG.info("Initiated csv reader");
        this.delimiter = job.get(LINE_DELIMITER, DEFAULT_DELIMITER);
        String columns = job.get("columns");
        if (columns.indexOf("__") != -1) {
            columns = columns.substring(0, columns.indexOf("__"));
        }
        columnNum = columns.split(",").length-1;
        LOG.info("columns:"+columns+"|columnNum:" + columnNum);
        System.out.println("columnNum:" + columnNum);
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", 10000);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            in = new BufferedReader(new InputStreamReader(codec.createInputStream(fileIn)));
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            in = new BufferedReader(new InputStreamReader(fileIn));
            //LOG.info("Initiated csv reader");
        }
        if (skipFirstLine) {  // skip first line and re-establish "start".
            Text text = new Text();
            start += readLine(text);
            LOG.info("at pos:"+start+" ,skip text:"+text);
        }
        this.pos = start;
    }

    @Override
    public synchronized boolean next(LongWritable longWritable, Text text) throws IOException {

        if (longWritable == null) {
            longWritable = new LongWritable();
        }
        longWritable.set(pos);

        if (text == null) {
            text = new Text();
        }
        while (pos <= end) {
            /*if(pos >= 33553500) {
                System.out.println();
            }*/
            long newSize = readLine(text);
            if (pos - start < 2000 || end - pos < 2000) {
                LOG.info("start:"+start+",end:"+end+",pos:"+pos+",size:"+newSize+"||"+text.toString());
            }
            if (newSize > maxLineLength) {
                LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
            }
            pos += newSize;
            if (newSize == 0) {
                return false;
            } else {
                return true;
            }

        }
        // line too long. try again
        return false;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public synchronized long getPos() throws IOException {
        return pos;
    }
    int tttt=0;

    /**
     * Skip if delimiter exist in between column
     */
    public long readLine(Text values) throws IOException {
        //LOG.info("Initiated csv line reader");

        values.clear();// Empty value columns list

        char c;
        long numRead = 0;
        boolean insideQuote = false;
        boolean currentLineInsideQuote = false;
        StringBuffer sb = new StringBuffer();
        StringBuffer currentLineStr = new StringBuffer();
        int i;
        int quoteOffset = 0, delimiterOffset = 0;
        Map<Integer, Long> lineNumReadMap = new HashMap<Integer, Long>();
        Map<Integer, String> lineStrMap = new HashMap<Integer, String>();
        int firstRightLineNum = 0;
        //采用标记的方式 来reset BufferedReader
        boolean isNeedMark = true;
        // Reads each char from input stream unless eof was reached
        int line =1;
        while ((i = in.read()) != -1) {
            c = (char) i;
            numRead++;
            sb.append(c);
            currentLineStr.append(c);
            // Check quotes, as delimiter inside quotes don't count
            if (c == DEFAULT_QUOTE.charAt(quoteOffset)) {
                insideQuote = !insideQuote;
                currentLineInsideQuote = !currentLineInsideQuote;
            }
            // Check delimiters, but only those outside of quotes
            // 增加 意外 处理 判断
            if (c == delimiter.charAt(0)) {
                lineNumReadMap.put(line, numRead);
                lineStrMap.put(line, sb.toString());

                if (!insideQuote) {
                    //整段内容 引号正常 没有问题
                    break;
                } else if(!currentLineInsideQuote) {
                    //整段内容 引号异常 当前 这一行 引号正确
                    if (columnNum <= getColumnNum(currentLineStr.toString())) {
                        //表示当前这一行的字段数是正确的 这一行的数据 正常
                        if (firstRightLineNum == 0) {
                            firstRightLineNum = line;
                            isNeedMark = false;
                        }
                        /*String beforeStr = lineStrMap.get(line - 1);
                        Long beforeNumRead = lineNumReadMap.get(line - 1);
                        LOG.info("special handle beforeStr:" + beforeStr + "|beforeNumRead:" + beforeNumRead);*/
                        if(line > 10) {
                            //如果超过10行 仍未完结整段内容， 此时认为当前这一行前的数据是一个整体
                            String beforefirstRightStr = lineStrMap.get(firstRightLineNum - 1);
                            Long beforefirstRightNumRead = lineNumReadMap.get(firstRightLineNum - 1);
                            values.set(beforefirstRightStr);
                            LOG.info("reset at:"+values.toString());
                            LOG.info("total:"+sb.toString());
                            in.reset();
                            return beforefirstRightNumRead;
                        }
                    }
                } else {
                    LOG.error("current line error:"+currentLineStr);
                    LOG.error("total line error:"+sb.toString());
                }
                currentLineInsideQuote = false;
                currentLineStr = new StringBuffer();
                line++;
                if(isNeedMark) {
                    in.mark(maxLineLength);
                }
            }

            /*if (!insideQuote) {
                // A new line outside of a quote is a real csv line breaker
                //not the CR LF ,now just LF \n for default
                if (c == delimiter.charAt(0)) {
                    break;
                }
            } */

            if (numRead > maxLineLength) {
                return numRead;
            }
        }
        if (!insideQuote) {
            //initColumnNum(sb.toString());
        } else if(!currentLineInsideQuote) {
            //整段内容 引号异常 当前 这一行 引号正确
            if (columnNum <= getColumnNum(currentLineStr.toString())) {
                //表示当前这一行的字段数是正确的 这一行的数据 正常
                if (firstRightLineNum == 0) {
                    firstRightLineNum = line;
                }
                //如果已结束 仍未完结整段内容， 此时认为firstRightLineNum行前的数据是一个整体
                String beforefirstRightStr = lineStrMap.get(firstRightLineNum - 1);
                Long beforefirstRightNumRead = lineNumReadMap.get(firstRightLineNum - 1);
                values.set(beforefirstRightStr);
                in.reset();
                return beforefirstRightNumRead;
            }
        } else {
            LOG.error("current line error:"+currentLineStr);
            LOG.error("total line error:"+sb.toString());
        }
        //LOG.info("line Read : " + sb.toString());
        values.set(sb.toString());

        return numRead;
    }

    private int tempColumnNum =0;
    public void initColumnNum(String rowStr) {
        if (columnNum > 0) {
            return;
        }
        if(tempColumnNum > 0) {
            int num = getColumnNum(rowStr);
            if (num > 0) {
                columnNum = tempColumnNum > num ? num : tempColumnNum;
            }
        } else {
            tempColumnNum = getColumnNum(rowStr);
        }
    }

    public int getColumnNum(String rowStr) {

        try {
            String[] columns = csvParser.parseLineMulti(rowStr);
            return columns.length;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
        }
        if (is != null) {
            is.close();
            is = null;
        }
    }
}