import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * Created by ken on 3/31/17.
 */
public class PriceLoader {

    public static void main(String[] args) throws Exception {

        System.out.println("Loading Prices");
        //LoadStockPrices(true);
        //LoadStockPrices(false);
        FileSystem aFS = FileSystem.get(new Configuration());

        Path aOutputPath = new Path("/Chips/PricesOut");
        if (aFS.exists(aOutputPath)) {
            aFS.delete(aOutputPath, true);
            System.out.println("Output folder removed");
        }

        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Loading Prices");
        job.setJarByClass(BruteController.class);

        job.setMapperClass(Mappers.PriceSplitter.class);
        job.setReducerClass(Mappers.StockResultCombination.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/Chips/Data/Raw"));
        job.setInputFormatClass(StockSplitter.class);
        FileOutputFormat.setOutputPath(job, new Path("/Chips/PricesOut"));
        job.submit();
        job.waitForCompletion(true);

    }

    public static void LoadStockPrices(String tType) throws Exception
    {
        Date aStart = new Date();
        String aLoadPath = "/Chips/StockList";
        File aStockFile = new File(aLoadPath);
        Path aPath = new Path(aLoadPath);
        FileSystem aFS = FileSystem.get(new Configuration());

        if (aFS.exists(aPath)) {
            //System.out.println("LOADING PROCESSED: " + tFileName);
            BufferedReader aReader = new BufferedReader(new InputStreamReader(aFS.open(aPath)));
            String aLine;
            aLine = aReader.readLine();
            ArrayList<String> aFile = new ArrayList<String>();
            while (aLine != null) {
                aFile.add(aLine);
                aLine = aReader.readLine();
                String aDeleteString ="/Chips/Data/Processed/"+aLine+"_"+tType + ".txt";
                Path aDeleteFile = new Path(aDeleteString);
                if (aFS.exists(aDeleteFile))
                {
                    aFS.delete(aDeleteFile, true);
                }
            }
            aReader.close();
        }
        Date aEnd = new Date();
        DateFormat aFormat = new SimpleDateFormat("HH:mm:ss");
        System.out.println("STARTED: " + aFormat.format(aStart));
        System.out.println("ENDED: " + aFormat.format(aEnd));
        Date aTime = new Date(aEnd.getTime() - aStart.getTime());
        System.out.println("Duration: " + aFormat.format(aTime));
    }

    public static void ProcessStock(String tTicker, String tType) throws Exception{
        int aDataStart = 180;
        int aInterval = 30;
        Stock GenStock = new Stock();
        try
        {
            GenStock.LoadRaw(tType, tTicker);
        }
        catch(Exception e)
        {
            System.out.println("Process Error: " +e.getMessage());
        }
        if (tType.equals("1d"))
        {
            aInterval = 10;
        }
        else if (tType.equals("15d"))
        {
            aInterval = 3;
        }
        else if (tType.equals("1y"))
        {
            aInterval = 2;
        }
        else if (tType.equals("10y"))
        {
            aInterval = 2;
        }


        GenStock.OutputToFile(tTicker+"_"+tType, aDataStart);
    }

    /*
    public static void ProcessMinuteStock(String tTicker) throws Exception
    {
        int aMinuteDataStart = 180; int aMinuteDataInterval = 30;
        Stock GenStock = new Stock();
        try
        {
            GenStock.LoadRawMinute(tTicker, false);
        }
        catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        GenStock.OutputToFile("Minute/"+tTicker, aMinuteDataStart, aMinuteDataInterval);
    }

    public static void ProcessDayStock(String tTicker) throws Exception
    {
        int aDayDataStart = 180; int aDayDataInterval = 10;
        Stock GenStock2 = new Stock();

        try
        {
            GenStock2.LoadRawDaily(tTicker);
        }
        catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        GenStock2.OutputToFile("Daily/"+tTicker, aDayDataStart, aDayDataInterval);
    }
    */
}

class LoaderThread implements Runnable
{
    public String m_Ticker;
    public boolean m_Minute;
    public LoaderThread(String tTicker, boolean tType)
    {
        m_Ticker = tTicker;
        m_Minute = tType;
    }


    public void run()
    {
        try
        {
            if (m_Minute)
            {
                //PriceLoader.ProcessMinuteStock(m_Ticker);
            }
            else
            {
                //PriceLoader.ProcessDayStock(m_Ticker);
            }
        }
        catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
    }

}