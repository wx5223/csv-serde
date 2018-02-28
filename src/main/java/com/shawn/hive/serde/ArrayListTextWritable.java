package com.shawn.hive.serde;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by shawn on 2018/2/27.
 */
public class ArrayListTextWritable extends ArrayList<Text> implements Writable {
    private static final long serialVersionUID = -6737762624115237320L;

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput dataoutput) throws IOException {
        dataoutput.writeInt(this.size());
        for (Text element : this) {
            element.write(dataoutput);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput datainput) throws IOException {
        this.clear();
        int count = datainput.readInt();
        for (int i = 0; i < count; i++) {
            try {
                Text obj = Text.class.newInstance();
                obj.readFields(datainput);
                this.add(obj);
            } catch (InstantiationException e) {
                throw new IOException(e);
            } catch (IllegalAccessException e) {
                throw new IOException(e);
            }
        }
    }

}