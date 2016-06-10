package jobs;

import misc.FrequencyCounter;
import misc.RemoveRedundantData;
import misc.StopWords;
import misc.TextArrayWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by cheta on 5/31/2016.
 */
public class MapReduce_Job1_TF {

    public static class M_Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable,Text,Text,TextArrayWritable>
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
        RemoveRedundantData removeRedundantData=new RemoveRedundantData();
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, TextArrayWritable> outputCollector, Reporter reporter) throws IOException {
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
                String s="";
                int endIndex = line.indexOf(text_end);
                String tmp = line.substring(0, endIndex);
                stringBuilder.append(tmp);
                addToBuffer = false;
                String redRemoved=removeRedundantData.removeRedundantData(stringBuilder.toString());
                //Tokenize the string, use either tokenizer or array split
                String tokens[]=redRemoved.split("\\s");
                List<String> stopRemovedList=new ArrayList<String>(Arrays.asList(tokens));//placeholder
                stopRemovedList.removeAll(StopWords.stopWordsList);//stop words removed
                stopRemovedList=removeRedundantData.doStemming(stopRemovedList);//stem the word and assign it to same list again
                //count the total terms in the array
//                int totalWordsInDocArray=stopRemovedList.size();
//                HashMap<String,Double> TF_HashMap=new FrequencyCounter().calculateTermFrequencies(stopRemovedList.toArray(new String[totalWordsInDocArray] ));//TF of each terms
                outputCollector.collect(new Text(ID),new TextArrayWritable(stopRemovedList));//output ID and the word
            }

            if (addToBuffer) //write to builder
            {
                stringBuilder.append(line);
            }
        }
    }

    /**
     * Reducer to extract the array
     * Reducer Class
     */
    public static class R_Reducer extends  MapReduceBase implements Reducer<Text,TextArrayWritable,Text,Text> {

        @Override
        public void reduce(Text text, Iterator<TextArrayWritable> iterator, OutputCollector<Text,
                Text> outputCollector, Reporter reporter) throws IOException {
            DecimalFormat df= new DecimalFormat("#");//using it to format the TF, otherwise it shows scientific notation
            df.setMaximumFractionDigits(20);
            while(iterator.hasNext())
            {
                TextArrayWritable textArrayWritable=iterator.next();
                HashMap<String,Double> TF_HashMap=new FrequencyCounter().calculateTermFrequencies(textArrayWritable.toStrings());//TF of each terms

                Set<String> keySet =TF_HashMap.keySet();//get all keys
                for (String setkey:keySet) {
                    outputCollector.collect(text,new Text(setkey+"\t"+df.format(TF_HashMap.get(setkey))));//output key and its Document Frequency
                }

//                outputCollector.collect(text,new Text(Arrays.toString(iterator.next().toStrings())));
            }

        }
    }
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(MapReduce_Job1_TF.class);
        conf.setJobName("MapReduce_BuildIndex");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(TextArrayWritable.class);
        conf.setMapperClass(MapReduce_Job1_TF.M_Mapper.class);
        conf.setReducerClass(MapReduce_Job1_TF.R_Reducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
