package jobs;

import misc.TextArrayWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Class for finding the IDF
 * Created by cheta on 6/8/2016.
 */
public class MapReduce_Job2_IDF {

    public static class M_Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable,Text,Text, Text>
    {
        private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String inputText=text.toString();
            String[] input=inputText.split("\\t");// TF[0]= ID,  TF[1]= term, TF[2]=term frequency
            if(input.length>2)
            {
                String sTemp=input[1].replaceAll("\\d","");
                if(sTemp.length()>1)
                    outputCollector.collect(new Text(input[1]),new Text(input[0]+"\t"+input[2]+"\t"+1));
            }
        }
    }

    /**
     * Reducer to extract the array
     * Reducer Class
     */
    public static class R_Reducer extends  MapReduceBase implements Reducer<Text,Text,Text,Text> {
        Integer totalNumOfDocs;
        @Override
        public void configure(JobConf job) {
            super.configure(job);
            totalNumOfDocs=job.getInt("TotalDocs",1);
        }
        @Override
        public void reduce(Text text, Iterator<Text> iterator, OutputCollector<Text,Text> outputCollector, Reporter reporter) throws IOException {

            int numberOfDocsWithTerm=0;
            String TF;
            while(iterator.hasNext())
            {
                String[] received=iterator.next().toString().split("\\t");
                System.out.println(Arrays.toString(received));
                numberOfDocsWithTerm+=Integer.parseInt(received[2]);
//                outputCollector.collect(text,new Text(Arrays.toString(received)));
            }

           double idf= 1 + Math.log(totalNumOfDocs / numberOfDocsWithTerm);
            outputCollector.collect(text,new Text(String.valueOf(idf)));

        }
    }
    public static void main(String[] args) throws Exception {
        File file= new File("inFiles/job1.txt");//for finding total number of documents/ID
        BufferedReader br= new BufferedReader(new FileReader(file));
        String sTemp="";
        ArrayList<String> list= new ArrayList<>();
        while((sTemp=br.readLine())!=null)
        {
            String[] data=sTemp.split("\\t");
            if(!list.contains(data[0]))
            {
                list.add(data[0]);
            }
        }

        System.out.println("total Documents: "+list.size());
        JobConf conf= new JobConf(MapReduce_Job2_IDF.class);
        conf.setInt("TotalDocs",list.size());
        conf.setJobName("MapReduce_IDF2");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(MapReduce_Job2_IDF.M_Mapper.class);
        conf.setReducerClass(MapReduce_Job2_IDF.R_Reducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
