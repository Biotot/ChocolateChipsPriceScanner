import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.print.attribute.standard.JobState;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ken on 3/31/17.
 */
public class JobContainer {
    public Date Start;
    public Date End;
    public String Type;
    public String Tag;
    public Job HadoopTask;
    public boolean Running;
    public double PercentChanged;

    public JobContainer(String tType)
    {
        Type = tType;
        Running=false;
        Tag = Type.split("_")[1];
        PercentChanged = 0;
    }

    public void AddJob() throws Exception
    {
        Start = new Date();

        if (!Type.contains("Rate"))
        {
            Util.CreateNewBrokerList(Type);
        }

        System.out.println("\nCreating Job for " + Type + "\n");
        HadoopTask = JobCreator();
        Running = true;
    }

    public void WaitForCompletion() throws Exception
    {
        if (Running)
        {
            System.out.println("\nCompleting " + Type + "\n");
            System.out.println("\nCompleting " + HadoopTask.getFinishTime() + "\n");
            System.out.println("\nCompleting " + HadoopTask.getTrackingURL() + "\n");
            System.out.println("\nCompleting " + HadoopTask.getHistoryUrl() + "\n");

            System.out.println("\nCompleting " + Type + " State: " + HadoopTask.getJobState() + "\n");

            if (HadoopTask.getJobState() != JobStatus.State.SUCCEEDED)
            {
                HadoopTask.waitForCompletion(true);
            }
            if (Type.contains("Broker"))
            {
                UpdateList();
            }
            else
            {
                RateStocks();
            }
            Running = false;
        }
    }


    public void UpdateList() throws Exception
    {

        System.out.println("Updating BrokerList");
        FileSystem aFS = FileSystem.get(new Configuration());
        Path aMasterBrokerPath = new Path("/Chips/Data/Brokers/"+Type+"Master.txt");
        Path aNewBrokerPath = new Path("/Chips/Data/Brokers/New"+Type+"List.txt");
        Path aFlagPath = new Path("/Chips/Flags/Refresh"+Type+"Master");
        String aLine;

        //Setting most of the output path
        String aOutputPath = "/Chips/"+Type+"Out/part-r-0000";
        HashMap<String, Double> aResultMap = new HashMap<String, Double>();
        for (int x=0; x<10; x++)
        {
            Path aResultsPath = new Path(aOutputPath+x);
            if (aFS.exists(aResultsPath)) {
                System.out.println(aResultsPath.toString());
                BufferedReader aResultsreader = new BufferedReader(new InputStreamReader(aFS.open(aResultsPath)));
                aLine = aResultsreader.readLine();
                while (aLine!=null) {
                    String[] aResults = aLine.split("\t");
                    if (aResultMap.containsKey(aResults[0]))
                    {
                        System.out.println("DUPES: " + aResults[0]);
                    }
                    aResultMap.put(aResults[0], Double.parseDouble(aResults[1]));
                    aLine = aResultsreader.readLine();
                }
                aResultsreader.close();
            }
            else
            {
                //System.out.println(aOutputPath + x + "Doesn't exist. wtf dude?");
                break;
            }
        }

        BufferedReader aNewBrokerReader = new BufferedReader(new InputStreamReader(aFS.open(aNewBrokerPath)));
        ArrayList<Broker> aBrokerList = new ArrayList<Broker>();
        ArrayList<Broker> aMasterBrokerList = new ArrayList<Broker>();
        aLine = aNewBrokerReader.readLine();
        while (aLine!=null) {
            Broker aBroker = new Broker(aLine);
            if (aResultMap.containsKey(aBroker.m_GUID.toString()))
            {
                aBroker.m_PercentReturn.set(aResultMap.get(aBroker.m_GUID.toString()));
                aBrokerList.add(aBroker);
            }
            aLine = aNewBrokerReader.readLine();
        }
        aNewBrokerReader.close();

        boolean aRefresh = aFS.exists(aFlagPath);
        if (aRefresh)
        {
            aFS.delete(aFlagPath, true);
        }
        if (aFS.exists(aMasterBrokerPath)) {
            BufferedReader aMasterBrokerReader = new BufferedReader(new InputStreamReader(aFS.open(aMasterBrokerPath)));
            aLine = aMasterBrokerReader.readLine();
            while (aLine != null) {
                if (!aRefresh)
                {
                    aBrokerList.add(new Broker(aLine));
                }
                aMasterBrokerList.add(new Broker(aLine));
                aLine = aMasterBrokerReader.readLine();
            }
            aMasterBrokerReader.close();
        }


        Collections.sort(aBrokerList, new Comparator<Broker>() {
            public int compare(Broker broker, Broker t1) {
                return t1.m_PercentReturn.compareTo(broker.m_PercentReturn);
            }
        });

        System.out.println("List Size:"+aBrokerList.size());
        //Delete all middle elements.
        aBrokerList.subList(1000, aBrokerList.size()-1000).clear();
        System.out.println("List Size:"+aBrokerList.size());

        int aUnchangedCount = 0;
        //This should be optimised out in the future, however at this point in the execution the host is idle
        for(Broker aMaster : aMasterBrokerList)
        {
            if (aBrokerList.contains(aMaster))
            {
                aUnchangedCount++;
            }
        }

        PercentChanged = ((double)(aUnchangedCount*100)/(double)aBrokerList.size());
        System.out.println("Percent Unchanged: "+ PercentChanged);
        if (aFS.exists(aMasterBrokerPath)) {
            aFS.delete(aMasterBrokerPath, true);
        }

        BufferedWriter aMasterWriter = new BufferedWriter(new OutputStreamWriter(aFS.create(aMasterBrokerPath)));
        for (int x=0; x<aBrokerList.size(); x++)
        {
            if (x<10 || x > aBrokerList.size()-10)
            {
                System.out.println(aBrokerList.get(x) + " : " + aBrokerList.get(x).m_PercentReturn.toString());
            }
            if (x<1000 || x > aBrokerList.size()-1001) {
                aMasterWriter.write(aBrokerList.get(x).SaveString() + "\n");
            }
        }
        aMasterWriter.close();

        End = new Date();
        DateFormat aFormat = new SimpleDateFormat("HH:mm:ss");
        System.out.println("STARTED: " + aFormat.format(Start.getTime()));
        System.out.println("ENDED: " + aFormat.format(End.getTime()));
        Date aTime = new Date(End.getTime() - Start.getTime());
        System.out.println("Duration: " + aFormat.format(aTime));
        System.out.println("Broker Cost: " + ((aTime.getTime()/1000.0)/10000) + "s");
    }

    public Job JobCreator() throws Exception
    {
        FileSystem aFS = FileSystem.get(new Configuration());

        Path aOutputPath = new Path("/Chips/"+Type+"Out");
        if (aFS.exists(aOutputPath)) {
            aFS.delete(aOutputPath, true);
            System.out.println("Output folder removed");
        }

        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        Configuration conf = new Configuration();

        if (aFS.exists(new Path("/Chips/Flags/Debug")))
        {
            conf.setInt("mapreduce.input.lineinputformat.linespermap", 1);
        }
        else
        {
            conf.setInt("mapreduce.input.lineinputformat.linespermap", 10);
        }
        Job job = Job.getInstance(conf, Type + "@" + PercentChanged);
        job.setJarByClass(BruteController.class);

        if (Type.equals("Broker_1d"))
        {
            job.setMapperClass(Mappers.Broker1d.class);
        }
        else if (Type.equals("Broker_15d"))
        {
            job.setMapperClass(Mappers.Broker15d.class);
        }
        else if (Type.equals("Broker_1y"))
        {
            job.setMapperClass(Mappers.Broker1y.class);
        }
        else if (Type.equals("Broker_10y"))
        {
            job.setMapperClass(Mappers.Broker10y.class);
        }
        else if (Type.equals("Rate_1d"))
        {
            job.setMapperClass(Mappers.Rate1d.class);
        }
        else if (Type.equals("Rate_15d"))
        {
            job.setMapperClass(Mappers.Rate15d.class);
        }
        else if (Type.equals("Rate_1y"))
        {
            job.setMapperClass(Mappers.Rate1y.class);
        }
        else if (Type.equals("Rate_10y"))
        {
            job.setMapperClass(Mappers.Rate10y.class);
        }

        job.setReducerClass(Mappers.BrokerCombination.class);
        job.setNumReduceTasks(2);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path("/Chips/StockList"));
        job.setInputFormatClass(NLineInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("/Chips/"+Type+"Out"));
        job.submit();

        return job;
    }

    public void RateStocks() throws Exception
    {

        FileSystem aFS = FileSystem.get(new Configuration());
        System.out.println("Rate Stocks");
        ArrayList<StockRating> aList = new ArrayList<StockRating>();

        //Setting most of the output path
        String aOutFile = "/Chips/"+Type+"Out/part-r-0000";
        HashMap<String, Double> aWinnerMap = new HashMap<String, Double>();
        HashMap<String, Double> aLoserMap = new HashMap<String, Double>();

        for (int x=0; x<10; x++)
        {
            String aLine;
            Path aResultsPath = new Path(aOutFile+x);
            if (aFS.exists(aResultsPath)) {
                System.out.println(aResultsPath.toString());
                BufferedReader aResultsreader = new BufferedReader(new InputStreamReader(aFS.open(aResultsPath)));
                aLine = aResultsreader.readLine();
                while (aLine!=null) {
                    String[] aResults = aLine.split("\t");

                    if (aResults[0].contains(Tag+"+"))
                    {
                        aWinnerMap.put(aResults[0], Double.parseDouble(aResults[1]));
                    }
                    else if (aResults[0].contains(Tag+"-"))
                    {
                        aLoserMap.put(aResults[0], Double.parseDouble(aResults[1]));
                    }
                    else
                    {
                        System.out.println("FAILED FILTER: " + aResults[0]);
                    }
                    aLine = aResultsreader.readLine();
                }
                aResultsreader.close();
            }
            else
            {
                break;
            }
        }

        System.out.println("WinnerCount: " + aWinnerMap.size());
        System.out.println("LoserCount: " + aLoserMap.size());
        for (String aKey : aWinnerMap.keySet())
        {
            String aKeyVal = aKey.substring(0, aKey.length()-1)+"-";
            if (aLoserMap.containsKey(aKeyVal))
            {
                StockRating aRating = new StockRating(aKey, aWinnerMap.get(aKey), aLoserMap.get(aKeyVal));
                if (aRating.Rating!=0)
                {
                    aList.add(aRating);
                }
            }
            else
            {
                System.out.println("This didn't have a match " + aKeyVal);
            }
        }

        System.out.println("ListCount: " + aList.size());

        Path aRatingPath = new Path("/Chips/Data/Brokers/"+Type+"Ratings.txt");
        if (aFS.exists(aRatingPath)) {
            aFS.delete(aRatingPath, true);
        }
        BufferedWriter aRatingWriter = new BufferedWriter(new OutputStreamWriter(aFS.create(aRatingPath)));

        Collections.sort(aList, new Comparator<StockRating>() {
            public int compare(StockRating first, StockRating second) {
                if (first.Rating < second.Rating) return -1;
                if (first.Rating > second.Rating) return 1;
                return 0;
            }
        });

        for (int x=0; x<aList.size(); x++)
        {
            if (aList.get(x).Rating!=0)
            {
                System.out.print(aList.get(x).toString());
                aRatingWriter.write(aList.get(x).toString());
            }
        }

        aRatingWriter.close();


    }
    public String toString()
    {
        if (Start!=null)
        {
            return Type+" "+Start.toString();
        }
        else
        {
            return Type;
        }
    }

}


