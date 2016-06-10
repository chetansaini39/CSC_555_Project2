package misc;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;

/**
 * Java class to serialize HashMap in Hadoop system
 * Created by chetan on 6/9/2016.
 */
public class HashMapWritable extends HashMap<Text,FloatWritable> implements Writable
{

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.write(size());
        //output  entries,
        for(HashMap.Entry<Text, FloatWritable> entry: entrySet())
        {
            entry.getKey().write(dataOutput);
            entry.getValue().write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
    clear();
        //read items size to expect
        int count=dataInput.readInt();
        //deserialize
        while(count --> 0)
        {
            Text key=new Text();
            key.readFields(dataInput);

            FloatWritable value=new FloatWritable();
            value.readFields(dataInput);

            put(key,value);

        }
    }
}
