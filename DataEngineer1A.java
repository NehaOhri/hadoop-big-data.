
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataEngineer
{
	public static class DEMapper extends Mapper<LongWritable,Text,Text,LongWritable>
	{

		LongWritable one =new LongWritable(1); 
		public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException
		{  	
		  if(key.get()> 0)
		  {
			String [] token=values.toString().split("\t"); 
			if(token[4]!=null && token[4].contains("DATA ENGINEER") && token[7]!=null && !token[7].equals("NA"))
	        {	
			  Text abc= new Text("DATA ENGINEER"+"\t"+token[7]);
			  context.write(abc,one);
			}
		  }
		}
	}
	public static class DEReducer extends Reducer <Text,LongWritable,Text,LongWritable> 
	 {   
		LongWritable SUM=new LongWritable(0); 
		int i=0; 
        String[] years={"2011","2012","2013","2014","2015","2016"}; 
        long [] arr=new long[1721]; 
        public void reduce(Text key,Iterable<LongWritable> values ,Context context) throws IOException, InterruptedException,ArrayIndexOutOfBoundsException
        { 
            long sum=0;
             for(LongWritable val:values)
             {
              sum+=val.get();
              arr[i++]=sum;
             }
            //context.write(key,new LongWritable(sum)); 
        }
        protected void cleanup(Context context) throws IOException, InterruptedException 
        { 
          for (int i=0;i<6;i++)
          {
            if (i==0)
            {
              context.write(new Text(years[i]),new LongWritable(arr[i]));
            }
            else
            {
              context.write(new Text(years[i]),new LongWritable((arr[i]-arr[i-1])*100/arr[i-1]));
            }
          }
        }
    }
    
	public static void main(String args[]) throws Exception,ArrayIndexOutOfBoundsException
	{
	Configuration conf= new Configuration();
	Job job= Job.getInstance(conf,"Question 1a");
	job.setJarByClass(DataEngineer.class);
	job.setMapperClass(DEMapper.class);
	job.setReducerClass(DEReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);
	FileInputFormat.setInputPaths(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true)?1:0);
	}
}
