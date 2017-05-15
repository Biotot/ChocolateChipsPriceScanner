import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.*;

/**
 * Created by ken on 3/31/17.
 */
public class BruteController {

    public static void main(String[] args) throws Exception {

        if (args.length==1)
        {
            if (args[0].equals("RateMinute"))
            {
                while(true)
                {
                    JobContainer aMinuteJob = new JobContainer("RateMinute");
                    aMinuteJob.AddJob();
                    aMinuteJob.WaitForCompletion();
                }
            }

        }
        else
        {
            RunLoop();
        }

    }

    public static void RunLoop() throws Exception
    {

        String aRunningStatus = "RUN";
        ArrayList<JobContainer> aTaskList = new ArrayList<JobContainer>();
        aTaskList.add(new JobContainer("Broker_1d"));
        aTaskList.add(new JobContainer("Broker_15d"));
        aTaskList.add(new JobContainer("Broker_1y"));
        aTaskList.add(new JobContainer("Broker_10y"));

        FileSystem aFS = FileSystem.get(new Configuration());
        Path aRateConstant = new Path("/Chips/Flags/RateConstant");

        while (aRunningStatus.equals("RUN"))
        {
            System.out.println(aTaskList);
            JobContainer aJob = aTaskList.get(0);
            aTaskList.remove(0);
            aJob.WaitForCompletion();

            Path aLoadPath = new Path("/Chips/Flags/Load_"+aJob.Tag);
            if (aFS.exists(aLoadPath))
            {
                PriceLoader.LoadStockPrices(aJob.Tag);
                aFS.delete(aLoadPath, true);
            }

            JobContainer aNewJob;
            Path aRatePath = new Path("/Chips/Flags/Rate_"+aJob.Tag);
            if (aFS.exists(aRatePath))
            {
                aNewJob = new JobContainer("Rate_"+aJob.Tag);
                if (!aFS.exists(aRateConstant))
                {
                    aFS.delete(aRatePath, true);
                }
            }
            else
            {
                aNewJob = new JobContainer("Broker_"+aJob.Tag);
            }
            aNewJob.PercentChanged = aJob.PercentChanged;
            aNewJob.AddJob();
            aTaskList.add(aNewJob);

        }

    }
}


