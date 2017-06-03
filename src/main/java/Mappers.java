import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import static org.apache.hadoop.metrics2.impl.MsInfo.Context;

/**
 * Created by ken on 3/31/17.
 */
public class Mappers {

    public static void Brute(int tInterval, String tVal, Mapper.Context tContext) throws IOException, InterruptedException {
        ArrayList<String> aValList = new ArrayList<String>(Arrays.asList(tVal.split("\n")));

        System.out.println(aValList.size() + "   COUNT");
        if (aValList.size()>0)
        {
            System.out.println(aValList.get(0));
        }
        Stock aStock = new Stock();
        String aSplit[] = aValList.get(0).split(",");
        String aTicker = aSplit[0];
        String aDate = aSplit[1].split(" ")[0];

        try {
            aStock.LoadProcessedArray(aSplit[0], aValList);
            if (aStock.Valid) {
                System.out.println(aSplit[0] + "   is Valid");
            }
        } catch (Exception e) {
            System.out.println("Error Thrown " + e.getMessage());
        }
        if (aStock.Valid) {
            //GenStock.OutputToFile(value.toString());
            FileSystem aFS = FileSystem.get(new Configuration());
            Path aPath = new Path("/Chips/Data/Brokers/NewBroker_" + tInterval + "List.txt");
            int count = 0;
            if (aFS.exists(aPath)) {
                BufferedReader aReader = new BufferedReader(new InputStreamReader(aFS.open(aPath)));
                String aLine;
                aLine = aReader.readLine();
                while (aLine != null) {
                    Broker aTestBroker = new Broker(aLine);

                    DoubleWritable aPercentRet = new DoubleWritable(aTestBroker.RunFull(aStock, tInterval));
                    Text aVal = new Text(aTestBroker.m_GUID);
                    if (aPercentRet.get() != -1) {
                        tContext.write(aVal, aPercentRet);
                    }

                    aLine = aReader.readLine();
                    count++;
                }

                aReader.close();
            }
            else
            {
                System.out.println(new Date() + " No fucking broker file: " + aPath.toString());
            }
            System.out.println(new Date() + " Completed: " + aTicker);
        } else {
            System.out.println(new Date() + " Invalid: " + aTicker);
        }
    }

    public static void Rate(int tInterval, String tStockName, Mapper.Context tContext) throws IOException, InterruptedException
    {
        FileSystem aFS = FileSystem.get(new Configuration());
        System.out.println(new Date() + " Started: " + tStockName);
        Stock aStock = new Stock(tStockName, tInterval+"");
        System.out.println(new Date() + " Loaded: " + tStockName);
        Path aFlagPath = new Path("/Chips/Flags/RateAll");
        if (aStock.Valid)
        {
            //Chips/Data/Brokers/Broker_1dMaster.txt
            Path aPath = new Path("/Chips/Data/Brokers/Broker_"+tInterval+"Master.txt");
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
                    Text aVal = new Text(tStockName+"_"+aTime+"_"+tInterval+"+");
                    DoubleWritable aBuyVal = new DoubleWritable(aWinnerBuyCount);
                    tContext.write(aVal, aBuyVal);

                    aVal = new Text(tStockName+"_"+aTime+"_"+tInterval+"-");
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

    public static class PriceSplitter
            extends Mapper<Object, Text, Text, IntWritable>
    {
        ArrayList<String> aDataList;
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            if (aDataList==null)
            {
                aDataList = new ArrayList<String>();
            }
            ArrayList<String> aValList = new ArrayList<String>(Arrays.asList(value.toString().split("\n")));

            System.out.println(aValList.size() + "   COUNT");
            Stock aStock = new Stock();
            String aSplit[] = aValList.get(0).split(",");
            String aDate = aSplit[1].split(" ")[0];
            try{
                aStock.LoadArray(aSplit[0], aDate,  aValList);
                if (aStock.Valid)
                {
                    System.out.println(aSplit[0] + "   is Valid");
                    aStock.OutputToFile(aSplit[0] + "_"+aDate, 180);
                    context.write(new Text(aSplit[0]), new IntWritable(1));
                }
            }
            catch(Exception e)
            {
                System.out.println("Error Thrown " + e.getMessage());
            }
            aDataList.add(aSplit[0] + " : " + aSplit[1]);
            System.out.println(value.toString() + "   END");
        }

        @Override
        protected void cleanup(Context context) throws IOException
        {
            HashMap<String, Integer> aMap = new HashMap<String, Integer>();
            if (aDataList!=null)
            {
                for (String aVal : aDataList)
                {
                    String aTicker = aVal.split(",")[0];
                    if (aMap.containsKey(aTicker))
                    {
                        aMap.put(aTicker, aMap.get(aTicker)+1);
                    }
                    else
                    {
                        aMap.put(aTicker, 1);
                    }
                }
            }
            System.out.println(aMap);

        }
    }

    public static class Broker15
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Brute(15, value.toString(), context);
        }
    }
    public static class Broker30
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Brute(30, value.toString(), context);
        }
    }
    public static class Broker60
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Brute(60, value.toString(), context);
        }
    }
    public static class Broker90
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Brute(90, value.toString(), context);
        }
    }
    public static class Rate15
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Rate(15, value.toString(), context);
        }
    }
    public static class Rate30
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Rate(30, value.toString(), context);
        }
    }
    public static class Rate60
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Rate(60, value.toString(), context);
        }
    }
    public static class Rate90
            extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map (Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Rate(90, value.toString(), context);
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
    public static class StockResultCombination
            extends Reducer<Text,DoubleWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int aTotal = 0;
            int aCount = 0;
            for(IntWritable val : values) {
                if (val.get()!=-1)
                {
                    aTotal+=val.get();
                    aCount++;
                }
            }
            if (aCount>0)
            {
                result.set(aTotal);
                context.write(key, result);
            }
        }
    }
}
