import java.io.*;
import org.apache.hadoop.fs.FileSystem;
import java.nio.file.*;
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

public class PatternRecordReader
extends RecordReader<LongWritable, Text> {

    private LineReader in;
    private final static Text EOL = new Text("\n");
    private Pattern delimiterPattern;
    private String delimiterRegex;
    private int maxLengthRecord;
    private long start;
    private long end;
    private long pos;
    private Text text = new Text();
    private String toSearch;
    private LongWritable key = new LongWritable();
    private Text lastMatch = new Text();
    @Override
    public void initialize(InputSplit inSplit,
        TaskAttemptContext context)
    throws IOException, InterruptedException {
        Configuration job = context.getConfiguration();
        FileSplit split = (FileSplit) inSplit;
        org.apache.hadoop.fs.Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        org.apache.hadoop.fs.FSDataInputStream fileIn = fs.open(split.getPath());
        //in = new LineReader(context.getConfiguration(),split);
        in = new LineReader(fileIn, job);
        this.delimiterRegex = job.get("record.delimiter.regex");
        this.toSearch = job.get("searchQuery");
        this.maxLengthRecord = job.getInt(
            "mapred.linerecordreader.maxlength",
            Integer.MAX_VALUE);

        delimiterPattern = Pattern.compile(delimiterRegex);
        start = (int)split.getStart();
        end = start + split.getLength();    
        pos = start;
    }

    private int readNext(Text text,
        int maxLineLength,
        int maxBytesToConsume)
    throws IOException {
      int offset = 0;
      text.clear();
      Text tmp = new Text();

      for (int i = 0; i < maxBytesToConsume; i++) {

        int offsetTmp = in.readLine(tmp,maxLineLength,maxBytesToConsume);
        offset += offsetTmp;
        pos = offset;
        key.set(pos);
        Matcher m = delimiterPattern.matcher(tmp.toString());

            // End of File
        if (offsetTmp == 0) {
            if(text.toString().indexOf(toSearch) >= 0)
                text.append(lastMatch.toString().getBytes(),0,lastMatch.getLength());
            else
                text.clear();
            break;
        }

        if (m.matches()) {
                // Record delimiter
            if(lastMatch == null){
                lastMatch = new Text();
                lastMatch.set(tmp);
            }
            else{
              lastMatch.clear();
              if(text.toString().indexOf(toSearch) >= 0){
                text.append(lastMatch.toString().getBytes(),0,lastMatch.getLength());
                text.append(lastMatch.toString().getBytes(),0,lastMatch.getLength());
            }
            else{
                text.clear();
                lastMatch.clear();
                }  
            }
            break;
        } else {
                // Append value to record
            text.append(EOL.getBytes(), 0, EOL.getLength());
            text.append(tmp.getBytes(), 0, tmp.getLength());
        }
    }
    return offset;
}

// int myReadNext(Text text) throws IOException{
//     Text temp = new Text();
//     text.clear();
//     int offset = 0;
//     int tempOffset = in.readLine(temp,1000,1000);
//     offset += tempOffset;
//     Matcher m = delimiterPattern.matcher(temp.toString());
//     if(m.matches()){
//         text.append(temp.getBytes(),0,temp.getLength());
//         while(!m.matches()){
//             tempOffset = in.readLine(temp,1000.1000);
//             if(tempOffset == 0){
//                 return 0;
//             }
//             text.append(temp.getBytes(),0,temp.getLength());
//             m = delimiterPattern.matcher(temp.toString());
//         }
//     }
// }

public boolean nextKeyValue() throws IOException, InterruptedException{
    key.set(pos);
    if(readNext(text,1000,20) == 0){
        return false;
    }
    return true;
}

public void close() throws IOException{
  if(in != null)
     in.close();
}

public LongWritable getCurrentKey() throws IOException, InterruptedException{
  return key;
}

public float getProgress() throws IOException{
    if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }	
}

public Text getCurrentValue() throws IOException, InterruptedException{
  return text;
}

}