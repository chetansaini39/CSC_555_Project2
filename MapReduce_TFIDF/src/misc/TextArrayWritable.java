package misc;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Created by cheta on 5/31/2016.
 */
public class TextArrayWritable extends ArrayWritable {
    Text[] textArray;
    public TextArrayWritable() {
        super(Text.class);

    }

    public TextArrayWritable(String[] stringArray)
    {
        super(Text.class);
        textArray=new Text[stringArray.length];
        for (int i = 0; i < stringArray.length ; i++) {
            textArray[i]=new Text(stringArray[i]);
        }
        set(textArray);
    }
    public TextArrayWritable(List<String> list)
    {
        super(Text.class);
       textArray=new Text[list.size()];
        for (int i = 0; i <list.size() ; i++)
        {
        textArray[i]=new Text(list.get(i));

        }
        set(textArray);
    }

    public TextArrayWritable(HashMap<String,Double> TF_HashMap)
    {
        super(Text.class);
        DecimalFormat df= new DecimalFormat("#");//using it to format the TF, otherwise it shows scientific notation
        df.setMaximumFractionDigits(20);
        textArray=new Text[TF_HashMap.size()];
        Set<String> mapKeys=TF_HashMap.keySet();
        int i=0;
        for (String k:mapKeys) {
        textArray[i]=new Text(k+"\t"+df.format(TF_HashMap.get(k)));
            i++;
        }
        set(textArray);
    }

    @Override
    public String[] toStrings() {
        return super.toStrings();
    }
}
