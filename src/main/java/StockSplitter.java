import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by ken on 5/26/17.
 */
public class StockSplitter extends FileInputFormat<LongWritable, Text> {
    public static final String LINES_PER_MAP = "mapreduce.input.lineinputformat.linespermap";

    public StockSplitter() {
    }

    public RecordReader<LongWritable, Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        context.setStatus(genericSplit.toString());
        return new MultiLineRecordReader();

        //return new CombineFileRecordReader();
    }

    public List<InputSplit> getSplits(JobContext job) throws IOException {
        ArrayList splits = new ArrayList();
        Iterator var4 = this.listStatus(job).iterator();

        while(var4.hasNext()) {
            FileStatus status = (FileStatus)var4.next();
            splits.addAll(getSplitsForFile(status, job.getConfiguration()));
        }

        return splits;
    }

    public static List<FileSplit> getSplitsForFile(FileStatus status, Configuration conf) throws IOException {
        ArrayList splits = new ArrayList();
        Path fileName = status.getPath();
        if(status.isDirectory()) {
            throw new IOException("Not a file: " + fileName);
        } else {
            FileSystem fs = fileName.getFileSystem(conf);
            LineReader aLineReader = null;

            try {
                FSDataInputStream in = fs.open(fileName);
                aLineReader = new LineReader(in, conf);
                Text line = new Text();
                int aLineCount = 0;
                long aBegin = 0L;
                long aLength = 0L;
                String aStockName = "Symbol";
                int aMapCount = 1;
                int aStockCount = 0;
                int var18;
                while((var18 = aLineReader.readLine(line)) > 0) {
                    aLineCount++;
                    String aCurrentName = line.toString().split(",")[0].split(" ")[0];
                    if (!aStockName.equals(aCurrentName))
                    {
                        if (!aStockName.equals("Symbol"))
                        {
                            aStockCount++;
                            //System.out.println(aCurrentName + " : " + aStockCount);
                            if (aStockCount==aMapCount)
                            {
                                splits.add(createFileSplit(fileName, aBegin, aLength));
                                aStockCount=0;
                                aBegin += aLength;
                                aLength = 0L;
                            }
                        }
                        aStockName = aCurrentName;
                    }
                    else
                    {
                    }
                    aLength += (long)var18;
                }

                if(aLineCount != 0) {
                    splits.add(createFileSplit(fileName, aBegin, aLength));
                }
            } finally {
                if(aLineReader != null) {
                    aLineReader.close();
                }

            }

            return splits;
        }
    }

    protected static FileSplit createFileSplit(Path fileName, long begin, long length) {
        return begin == 0L?new FileSplit(fileName, begin, length - 1L, new String[0]):new FileSplit(fileName, begin - 1L, length, new String[0]);
    }


    public static class MultiLineRecordReader extends RecordReader<LongWritable, Text>{
        private int NLINESTOPROCESS;
        private LineReader in;
        private LongWritable key;
        private Text value = new Text();
        private long start =0;
        private long end =0;
        private long pos =0;
        private int maxLineLength;
        private String m_StockName;

        @Override
        public void close() throws IOException {
            if (in != null) {
                in.close();
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException,InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (start == end) {
                return 0.0f;
            }
            else {
                return Math.min(1.0f, (pos - start) / (float)(end - start));
            }
        }

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context)throws IOException, InterruptedException {
            NLINESTOPROCESS = 900;
            FileSplit split = (FileSplit) genericSplit;
            final Path file = split.getPath();
            Configuration conf = context.getConfiguration();
            this.maxLineLength = conf.getInt("mapreduce.input.linerecordreader.line.maxlength",Integer.MAX_VALUE);
            FileSystem fs = file.getFileSystem(conf);
            start = split.getStart();
            end= start + split.getLength();
            boolean skipFirstLine = false;
            FSDataInputStream filein = fs.open(split.getPath());

            if (start != 0){
                skipFirstLine = true;
                --start;
                filein.seek(start);
            }
            in = new LineReader(filein,conf);
            if(skipFirstLine){
                Text aText = new Text();
                start += in.readLine(aText,0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
                m_StockName = aText.toString().split(",")[0];
            }
            this.pos = start;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (key == null) {
                key = new LongWritable();
            }
            key.set(pos);
            if (value == null) {
                value = new Text();
            }
            value.clear();
            final Text endline = new Text("\n");
            int newSize = 0;
            for(int i=0;i<NLINESTOPROCESS;i++){
                Text v = new Text();
                while (pos < end) {
                    newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
                    //String aSplit[] = v.toString().split(",");
                    //if (m_StockName.equals(aSplit[0]))
                    {
                        value.append(v.getBytes(),0, v.getLength());
                        value.append(endline.getBytes(),0, endline.getLength());
                        if (newSize == 0) {
                            break;
                        }
                        pos += newSize;
                        if (newSize < maxLineLength) {
                            break;
                        }
                    }
                    /*else
                    {
                        i=NLINESTOPROCESS;
                        m_StockName = aSplit[0];
                        break;
                    }*/

                }
            }
            if (newSize == 0) {
                key = null;
                value = null;
                return false;
            } else {
                return true;
            }
        }
    }

}
