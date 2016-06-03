import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Mapreduce for query: SELECT EFirst, ELast, EID, CID, Address
 FROM Employee, Customer
 WHERE EFirst = CFirst AND ELast = CLast;

 * Created by cheta on 6/2/2016.
 */
public class MapReduceJob_Ass6_Q3 {

    public static  class M_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String stringInput=text.toString();
            //first identify weather its a record from Employee or the Customer Table
            String[] splits= stringInput.split("\\|");//split by pipe |
            String empId_custId=splits[0];
            String firstName=splits[1];
            String lastName=splits[2];
            String address_Age=splits[3];
            outputCollector.collect(new Text(firstName+" "+lastName),new Text(empId_custId+"|"+address_Age));



        }
    }

    public static  class  R_Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
    {

        @Override
        public void reduce(Text text, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String EID="", CID="", ADDRESS="";
            while (iterator.hasNext())
            {
                StringBuilder sb= new StringBuilder();
                String[] strings=iterator.next().toString().split("\\|");
                String eid_cid=strings[0];
                String add_age=strings[1];
            // if new EID is ==old EID
                if(EID.length()>1 && CID.length()>1)//they are not null
                {
                    if((!eid_cid.equals(EID)) || !eid_cid.equals(CID))//value not equal to old value
                    {
                        sb.append(EID+"|");
                        sb.append(CID+"|");
                        sb.append(ADDRESS);
                        outputCollector.collect(text,new Text(sb.toString()));
                    }
                }
                else
                {
                    if(eid_cid.startsWith("EMP"))
                    {
                        if(!eid_cid.equals(EID))
                        EID=strings[0];
                    }
                    else
                    if(!eid_cid.equals(CID))
                        CID=strings[0];
                        ADDRESS=strings[1];
                }

            }
        }
    }
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(MapReduceJob_Ass6_Q3.class);
        conf.setJobName("MapReduce_BuildIndex");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(MapReduceJob_Ass6_Q3.M_Mapper.class);
        conf.setReducerClass(MapReduceJob_Ass6_Q3.R_Reducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
