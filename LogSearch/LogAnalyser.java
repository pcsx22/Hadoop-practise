import java.io.*;
//import java.io.*;
import java.util.regex.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class LogAnalyser{
	
	public static class Map extends Mapper<LongWritable,Text,Text,NullWritable>{
//		Text t = new Text("sum");
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			//String line = value.toString();
			context.write(new Text(key + "---" + value),NullWritable.get());
		}
	}	


	public static void main(String[] args) throws Exception{
		String regex = "^[A-Za-z]{3},\\s\\d{2}\\s[A-Za-z]{3}.*";
		Configuration conf = new Configuration();
		conf.set("searchQuery",args[2]);
		conf.set("record.delimiter.regex", regex);
		Job job = new Job(conf,"log analysis");
		job.setInputFormatClass(PatternInputFormat.class);
		job.setJarByClass(LogAnalyser.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(Map.class);
//		job.setCombinerClass(Reduce.class);
//		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
	}
	

}

class PatternInputFormat
        extends FileInputFormat<LongWritable,Text>{
 
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split,
            TaskAttemptContext context)
                           throws IOException,
                      InterruptedException {
 
        return new PatternRecordReader();
    }
 
}
