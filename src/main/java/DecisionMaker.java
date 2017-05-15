import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by ken on 4/11/17.
 */
public class DecisionMaker {

    public static HashMap<String, HashMap<String, StockRating>> m_RatingsMap;
    public static void main(String[] args) throws Exception {


        if (args.length==1) {
            if (args[0].equals("RateRatings"))
            {
                RateRatings();
            }
            else if (args[0].equals("RateCurrent"))
            {
                ScoreRatings();
            }
        }
        else
        {
            System.out.println("You need args dude. Use 'RateRatings' or 'RateCurrent'");
        }
    }

    public static void LoadMaps() throws Exception {
        FileSystem aFS = FileSystem.get(new Configuration());
        String aLoadPath = "/Chips/StockList";
        Path aListPath = new Path(aLoadPath);

        m_RatingsMap = new HashMap<String, HashMap<String, StockRating>>();
        m_RatingsMap.put("1d", new HashMap<String, StockRating>());
        m_RatingsMap.put("15d", new HashMap<String, StockRating>());
        m_RatingsMap.put("1y", new HashMap<String, StockRating>());
        m_RatingsMap.put("10y", new HashMap<String, StockRating>());

        Iterator aMapItter = m_RatingsMap.entrySet().iterator();
        while (aMapItter.hasNext())
        {
            Map.Entry aPair = (Map.Entry)aMapItter.next();
            HashMap<String, StockRating> aMap = (HashMap<String, StockRating>)aPair.getValue();

            String aRatingsPath = "/Chips/Data/Brokers/Rate_"+aPair.getKey()+"Ratings.txt";//: "DayRatings.txt");
            Path aRatingsFile = new Path(aRatingsPath);
            if (aFS.exists(aRatingsFile)) {
                BufferedReader aReader = new BufferedReader(new InputStreamReader(aFS.open(aRatingsFile)));
                String aLine;
                aLine = aReader.readLine();
                while (aLine!=null) {
                    String[] aResults = aLine.split(",");
                    aMap.put(aResults[0], new StockRating(aResults[0], Double.parseDouble(aResults[2]), Double.parseDouble(aResults[3])));
                    aLine = aReader.readLine();
                }

                System.out.println(aPair.getKey() + " size: "+aMap.size());
            }

        }

    }

    public static void ScoreRatings() throws Exception{
        LoadMaps();
        ArrayList<StockRating> aRatingList = new ArrayList<StockRating>();
        FileSystem aFS = FileSystem.get(new Configuration());
        String aLoadPath = "/Chips/StockList";
        ArrayList<String> aTypeList = new ArrayList<String>();
        aTypeList.add("1d");aTypeList.add("15d");aTypeList.add("1y");aTypeList.add("10y");
        Path aListPath = new Path(aLoadPath);
        if (aFS.exists(aListPath)) {
            System.out.println("LOADING LIST");
            BufferedReader aReader = new BufferedReader(new InputStreamReader(aFS.open(aListPath)));
            String aLine;
            aLine = aReader.readLine();
            int aCount = 0;
            int a1dTotal = 0; int a1dCount = 0;
            int a15dTotal  = 0; int a15dCount = 0;
            int a1yTotal = 0; int a1yCount = 0;
            int a10yTotal = 0; int a10yCount = 0;

            while (aLine != null) {
                //System.out.println(aLine);
                int aMinuteScore = 0;
                int aDayScore = 0;
                for (String aType : aTypeList)
                {
                    Stock aStock = new Stock(aLine, aType);
                    if (aStock.Valid)
                    {
                        String aTime = aStock.Prices.Values.get(aStock.Prices.Values.size()-1).Time;
                        String aKey = aLine + "_" + aTime + "_" + aType + "+";
                        if (m_RatingsMap.get(aType).containsKey(aKey))
                        {
                            if (aType.contains("d"))
                            {
                                aMinuteScore += m_RatingsMap.get(aType).get(aKey).Rating;
                            }
                            else
                            {
                                aDayScore += m_RatingsMap.get(aType).get(aKey).Rating;
                            }

                            if (aType.equals("1d"))
                            {
                                a1dTotal += m_RatingsMap.get(aType).get(aKey).Rating;
                                a1dCount++;
                            }
                            else if (aType.equals("15d"))
                            {
                                a15dTotal += m_RatingsMap.get(aType).get(aKey).Rating;
                                a15dCount++;
                            }
                            else if (aType.equals("1y"))
                            {
                                a1yTotal += m_RatingsMap.get(aType).get(aKey).Rating;
                                a1yCount++;
                            }
                            else if (aType.equals("10y"))
                            {
                                a10yTotal += m_RatingsMap.get(aType).get(aKey).Rating;
                                a10yCount++;
                            }
                        }
                    }
                }

                StockRating aRating = new StockRating(aLine+"[MD]", aMinuteScore, aDayScore, true);
                aRatingList.add(aRating);
                aLine = aReader.readLine();
            }

            double a1dAverage = (a1dCount==0)? 0 : a1dTotal/a1dCount;
            double a15dAverage = (a15dCount==0)? 0 : a15dTotal/a15dCount;
            double a1yAverage = (a1yCount==0)? 0 : a1yTotal/a1yCount;
            double a10yAverage = (a10yCount==0)? 0 : a10yTotal/a10yCount;
            StockRating aDeltaRating = new StockRating("DeltaRating", -(a1dAverage+a15dAverage), -(a1yAverage+a10yAverage));
            for (StockRating aRating : aRatingList)
            {
                aRating.Combine(aDeltaRating);
            }

        }


        Collections.sort(aRatingList, new Comparator<StockRating>() {
            public int compare(StockRating first, StockRating second) {
                if (first.Rating < second.Rating) return -1;
                if (first.Rating > second.Rating) return 1;
                return 0;
            }
        });

        for (StockRating aRating : aRatingList)
        {
            System.out.print(aRating);
        }
    }

    public static void RateRatings() throws Exception {

        Date aStart = new Date();
        LoadMaps();
        FileSystem aFS = FileSystem.get(new Configuration());
        String aLoadPath = "/Chips/StockList";
        Path aListPath = new Path(aLoadPath);

        ArrayList<RatingConfig> aConfigList = new ArrayList<RatingConfig>();
        RatingConfig aConfig = new RatingConfig();
        aConfig.m_Key[0] = 500;
        aConfig.m_Key[1] = -500;
        aConfigList.add(aConfig);

        if (aFS.exists(aListPath)) {
            System.out.println("LOADING LIST");
            BufferedReader aReader = new BufferedReader(new InputStreamReader(aFS.open(aListPath)));
            String aLine;
            aLine = aReader.readLine();
            double a1dPercentRet = 0;
            double a15dPercentRet = 0;
            double a1yPercentRet = 0;
            double a10yPercentRet = 0;
            int aCount = 0;
            while (aLine!=null) {
                a1dPercentRet += RunReturns(aConfigList, aLine, "1d");
                a15dPercentRet += RunReturns(aConfigList, aLine, "15d");
                a1yPercentRet += RunReturns(aConfigList, aLine, "1y");
                a10yPercentRet += RunReturns(aConfigList, aLine, "10y");
                aCount++;
                aLine = aReader.readLine();
            }

            a1dPercentRet = a1dPercentRet/aCount;
            a15dPercentRet = a15dPercentRet/aCount;
            a1yPercentRet = a1yPercentRet/aCount;
            a10yPercentRet = a10yPercentRet/aCount;
            System.out.println("1d PercentRet: "+a1dPercentRet);
            System.out.println("15d PercentRet: "+a15dPercentRet);
            System.out.println("1y PercentRet: "+a1yPercentRet);
            System.out.println("10y PercentRet: "+a10yPercentRet);
        }
    }

    public static double RunReturns(ArrayList<RatingConfig> tConfigList, String tStockName, String tType) throws Exception
    {
        Stock aStock = new Stock(tStockName, tType);
        if (aStock.Valid)
        {
            for(RatingConfig aConfig : tConfigList)
            {
                aConfig.m_PercentReturn = CalcReturns(aConfig, aStock, tType, tStockName);
            }

            Collections.sort(tConfigList, new Comparator<RatingConfig>() {
                public int compare(RatingConfig t1, RatingConfig t2) {
                    return ((Double)t2.m_PercentReturn).compareTo(t1.m_PercentReturn);
                }
            });

            RatingConfig aConfig = tConfigList.get(0);
            String aPrintString = tStockName + "\t:" + aConfig + "\t%.5f\t" + tType + "\n";
            if (aConfig.m_PercentReturn!=1)
            {
                //System.out.printf(aPrintString, aConfig.m_PercentReturn);
            }
        }
        return tConfigList.get(0).m_PercentReturn;
    }

    public static double CalcReturns(RatingConfig tConfig, Stock tStock, String tType, String tStockName) {
        double aRet = 0;

        StockSegment aDay = tStock.Prices;
        double aInvestment = 0;
        double aSellValue = 0;
        int aShareCount = 0;
        for (int y = 1; y < aDay.Values.size() - 2; y++) {
            StockRating aRating = null;
            StockPrice aPrice = aDay.Values.get(y);
            String aKeyString = tStockName+"_" + aPrice.Time+"_"+tType+"+";

            HashMap<String, StockRating> aMap = m_RatingsMap.get(tType);
            if (aMap.containsKey(aKeyString))
            {
                aRating = aMap.get(aKeyString);
            }

            if (aRating!=null)
            {
                if (aRating.Rating > tConfig.m_Key[0])
                {
                    aInvestment+=aPrice.Close;
                    aShareCount++;
                }
                else if (aRating.Rating < tConfig.m_Key[1])
                {
                    if (aShareCount>0)
                    {
                        aSellValue+=aPrice.Close;
                        aShareCount--;
                    }
                }

            }
        }
        if (aShareCount>0)
        {
            aSellValue += aDay.Values.get(aDay.Values.size()-1).Close*aShareCount;
        }

        if (aInvestment>0)
        {
            aRet = aSellValue/aInvestment;
        }
        else
        {
            aRet = 1;
        }
        return aRet;
    }
}



class RatingConfig{

    public int m_KeyCount = 5;
    public double[] m_Key = new double[m_KeyCount];
    public double m_PercentReturn;

    public RatingConfig()
    {
        m_PercentReturn = 0;
    }

    public String toString()
    {
        String aRet;
        aRet = "["+m_Key[0] + "\t" + m_Key[1] + "]";
        return aRet;
    }
}