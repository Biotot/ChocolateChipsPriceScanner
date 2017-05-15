import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;

import static org.apache.hadoop.metrics2.impl.MsInfo.Context;

/**
 * Created by ken on 3/31/17.
 */
public class Mappers {

    public static void Brute(String tJobType, String tStockName, Mapper.Context tContext) throws IOException, InterruptedException
    {
        System.out.println(new Date() + " Started: " +tStockName);
        Stock aStock = new Stock(tStockName, tJobType);
        System.out.println(new Date() + " Loaded: " + tStockName);
        if (aStock.Valid)
        {
            //GenStock.OutputToFile(value.toString());
            FileSystem aFS = FileSystem.get(new Configuration());
            Path aPath = new Path("/Chips/Data/Brokers/NewBroker_"+tJobType+"List.txt");
            int count = 0;
            if (aFS.exists(aPath))
            {
                BufferedReader aReader = new BufferedReader(new InputStreamReader(aFS.open(aPath)));
                String aLine;
                aLine = aReader.readLine();
                while (aLine!=null)
                {
                    Broker aTestBroker = new Broker(aLine);

                    DoubleWritable aPercentRet = new DoubleWritable(aTestBroker.RunFull(aStock));
                    Text aVal = new Text(aTestBroker.m_GUID);
                    if (aPercentRet.get()!=-1)
                    {
                        tContext.write(aVal, aPercentRet);
                    }

                    aLine=aReader.readLine();
                    count++;
                }

                aReader.close();
            }
            System.out.println(new Date() + " Completed: " + tStockName);
        }
        else
        {
            System.out.println(new Date() + " Invalid: " + tStockName);
        }
    }

    public static void Rate(String tJobType, String tStockName, Mapper.Context tContext) throws IOException, InterruptedException
    {
        FileSystem aFS = FileSystem.get(new Configuration());
        System.out.println(new Date() + " Started: " + tStockName);
        Stock aStock = new Stock(tStockName, tJobType);
        System.out.println(new Date() + " Loaded: " + tStockName);
        Path aFlagPath = new Path("/Chips/Flags/RateAll");
        if (aStock.Valid)
        {
            //Chips/Data/Brokers/Broker_1dMaster.txt
            Path aPath = new Path("/Chips/Data/Brokers/Broker_"+tJobType+"Master.txt");
            ArrayList<Broker> aBrokerList = new ArrayList<Broker>();
            if (aFS.exists(aPath))
            {
                BufferedReader aReader = new BufferedReader(new InputStreamReader(aFS.open(aPath)));
                String aLine;
                aLine = aReader.readLine();

                StockSegment aSegment = aStock.Prices;
                int aStartIndex = aSegment.Values.size()-1;
                if (aFS.exists(aFlagPath))
                {
                    aStartIndex = 1;
                }
                while (aLine!=null)
                {
                    Broker aTestBroker = new Broker(aLine);
                    aBrokerList.add(aTestBroker);
                    aLine = aReader.readLine();
                }
                aReader.close();
                for (int y=aStartIndex; y<aSegment.Values.size(); y++)
                {
                    double aWinnerBuyCount = 0;
                    double aLoserBuyCount = 0;
                    for (int x=0; x<aBrokerList.size(); x++) {
                        if (aBrokerList.get(x).Evaluate(aSegment.PercentChanges, y)) {
                            if (x < 1000) {
                                aWinnerBuyCount++;
                            } else {
                                aLoserBuyCount++;
                            }
                        }
                    }
                    String aTime = aSegment.Values.get(y).Time;
                    Text aVal = new Text(tStockName+"_"+aTime+"_"+tJobType+"+");
                    DoubleWritable aBuyVal = new DoubleWritable(aWinnerBuyCount);
                    tContext.write(aVal, aBuyVal);

                    aVal = new Text(tStockName+"_"+aTime+"_"+tJobType+"-");
                    aBuyVal = new DoubleWritable(aLoserBuyCount);
                    tContext.write(aVal, aBuyVal);

                }

            }
        }
        else
        {
            System.out.println("Stock List Invalid: " + tStockName);
        }
        System.out.println(new Date() + " Completed: " + tStockName);
    }

    public static class Broker1d
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String aType = "1d";
            Brute(aType, value.toString(), context);
        }
    }
    public static class Broker15d
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String aType = "15d";
            Brute(aType, value.toString(), context);
        }
    }
    public static class Broker1y
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String aType = "1y";
            Brute(aType, value.toString(), context);
        }
    }
    public static class Broker10y
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String aType = "10y";
            Brute(aType, value.toString(), context);
        }
    }
    public static class Rate1d
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String aType = "1d";
            Rate(aType, value.toString(), context);
        }
    }
    public static class Rate15d
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String aType = "15d";
            Rate(aType, value.toString(), context);
        }
    }
    public static class Rate1y
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String aType = "1y";
            Rate(aType, value.toString(), context);
        }
    }
    public static class Rate10y
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String aType = "10y";
            Rate(aType, value.toString(), context);
        }
    }

    public static class BrokerCombination
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {


            double aTotal = 0;
            int aCount = 0;
            for(DoubleWritable val : values) {
                if (val.get()!=-1)
                {
                    aTotal+=val.get();
                    aCount++;
                }
            }
            if (aCount>0)
            {
                result.set(aTotal/aCount);
                context.write(key, result);
            }
        }
    }
}
