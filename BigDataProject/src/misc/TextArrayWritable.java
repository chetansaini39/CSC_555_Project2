package misc;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import java.util.List;

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

    @Override
    public String[] toStrings() {
        return super.toStrings();
    }
}
