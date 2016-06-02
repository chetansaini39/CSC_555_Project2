package jobs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by cheta on 6/1/2016.
 */
public class TestJob {




    public static class M_Mapper extends MapReduceBase implements Mapper<LongWritable,//input key type
            Text,//input value type
            Text,//output key type
            Text>//output value
    {
        String title_beg = "<title>";
        String title_end = "</title>";
        String id_beg = "<id>";
        String id_end = "</id>";
        String text_beg = "<text>";
        String text_end = "</text>";
        int beginIndex=0;
        int endIndex=0;
        String ID="ID";
        boolean addToBuffer=false;
        StringBuilder stringBuilder= new StringBuilder();
        String TITLE="TITLE";
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

            String line= text.toString();
            line=line.toLowerCase();//convert to lowercase
            if(line.contains(id_beg))// ID found
            {
                beginIndex=line.indexOf(id_beg);//begin index of ID
                endIndex=line.indexOf(id_end);//end index of end tag
                ID=line.substring(beginIndex+id_beg.length(),endIndex);//gives ID
            }
            else if(line.contains(title_beg))
            {
                beginIndex=line.indexOf(title_beg);//begin index of ID
                endIndex=line.indexOf(title_end);//end index of end tag
                TITLE=line.substring(beginIndex+title_beg.length(),endIndex);//gives ID
            }
            else if(line.contains(text_beg))
            {
                addToBuffer=true;
                beginIndex=line.indexOf(text_beg);
                stringBuilder.append(beginIndex + text_beg.length());//add the data after <text> tag to temp

            }
            else if (line.contains(text_end))//end tag </text> found
            {
                int endIndex = line.indexOf(text_end);
                String tmp = line.substring(0, endIndex);
                stringBuilder.append(tmp);
                addToBuffer = false;
                String data=ID+"|"+TITLE;
                outputCollector.collect(new Text(data),new Text(TITLE));
            }

            if (addToBuffer) //write to builder
            {
                stringBuilder.append(line);
            }

        }
    }

    public static class R_Reducer extends  MapReduceBase implements Reducer<Text,IntWritable,Text,Text> {

        @Override
        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text,
                Text> outputCollector, Reporter reporter) throws IOException {

            outputCollector.collect(text,new Text(""));
        }
    }
    public static void main(String[] args) throws Exception{
        JobConf conf= new JobConf(TestJob.class);
        conf.setJobName("ID and title");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(TestJob.M_Mapper.class);
        conf.setReducerClass(TestJob.R_Reducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }


}
