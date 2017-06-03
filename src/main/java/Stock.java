import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by ken on 3/31/17.
 */

//https://chart.finance.yahoo.com//instrument/1.0/MU/chartdata;type=quote;range=15d/csv


public class Stock {

    public StockSegment Prices;
    public boolean Valid;

    public Stock() {}

    public Stock(String tTicker, String tType)
    {
        Valid = false;
        try {
            LoadProcessed(tTicker, tType);
            if (!Valid)
            {
                System.out.println("Stock is invalid:  " + tTicker);
            }
        }
        catch(Exception e) {
            System.out.println("SHIT FUCKED UP:  " + e.getMessage() + " for stock: " + tTicker + "_" + tType);
        }
    }
    //https://chart.finance.yahoo.com//instrument/1.0/MU/chartdata;type=quote;range=15d/csv
    public void LoadRaw(String tType, String tStockName) throws IOException {
        BufferedReader aReader = null;
        //https://www.google.com/finance/getprices?i=60&p=1d&f=d,o,h,l,c,v&df=cpct&q=AMD
        String aLoadURL = String.format("https://chart.finance.yahoo.com//instrument/1.0/%s/chartdata;type=quote;range=1d/csv", tStockName);
        System.out.println(aLoadURL);
        URL aAPI = new URL(aLoadURL);
        InputStream aInput = aAPI.openStream();
        aReader = new BufferedReader(new InputStreamReader(aInput));

        String aLine;
        aLine = aReader.readLine();
        ArrayList<String> aFile = new ArrayList<String>();
        while (aLine != null) {
            aFile.add(aLine);
            aLine = aReader.readLine();
        }
        aReader.close();
        if (aFile.size() > 100) {
            Prices = new StockSegment();
            for (int x = 0; x < aFile.size(); x++) {
                //Find the start of a day
                if ((!aFile.get(x).contains("labels"))&&(!aFile.contains("values"))&&(!aFile.get(x).contains("close"))) {
                    if (aFile.get(x).split(",").length == 6) {
                        //System.out.println(aFile.get(x));
                        StockPrice aPrice = new StockPrice(aFile.get(x));
                        Prices.Values.add(aPrice);
                    }
                }
            }
            if (Prices.Values.size()>100)
            {
                Valid = true;
                //System.out.println("Size correct: " + Prices.Values.size());
            }
            else {
                //System.out.println("Size too small for stock: " + Prices.Values.size());
            }
            Prices.Process();
        }
        else
        {
            System.out.println("File length too short: " + aFile.size());
            Valid = false;
        }
    }

    public void LoadProcessed(String tTicker, String tType) throws IOException, Exception
    {
        FileSystem aFS = FileSystem.get(new Configuration());

        Path aProcessedPath = new Path("/Chips/Data/Processed/" + tTicker + "_"+tType + ".txt");
        if (!aFS.exists(aProcessedPath)) {
            System.out.println("Loading: " + tTicker);
            PriceLoader.ProcessStock(tTicker, tType);
        }

        if (aFS.exists(aProcessedPath)) {
            //System.out.println("LOADING PROCESSED: " + tFileName);
            BufferedReader aReader = new BufferedReader(new InputStreamReader(aFS.open(aProcessedPath)));
            String aLine;
            aLine = aReader.readLine();
            ArrayList<String> aFile = new ArrayList<String>();
            int count = 0;
            while (aLine!=null)
            {
                aFile.add(aLine);
                aLine=aReader.readLine();
                count++;
            }
            aReader.close();

            if (aFile.size()>50)
            {
                Prices = new StockSegment(true);
                for (int x=0; x<aFile.size(); x++)
                {
                    //Find the start of a day
                    if (aFile.get(x).length()>0 ){
                        Prices.LoadFullMinute(aFile.get(x));
                        if (Prices.Values.size()==1)
                        {
                            Prices.Date = Prices.Values.get(0).Time;
                            //System.out.println(aDay.Date);
                        }

                    }
                }
            }
        }
        else
        {
            System.out.println("FILE NOT FOUND: " + tTicker);
        }
        if (Prices.Values.size()>50)
        {
            Valid = true;
        }
        else
        {
            System.out.println("Stock data is insufficient");
        }
    }

    public void LoadProcessedArray(String tTicker, ArrayList<String> tFile) throws IOException, Exception {

        int count = 0;

        if (tFile.size() > 2) {
            Prices = new StockSegment(true);
            for (int x = 0; x < tFile.size(); x++) {
                //Find the start of a day
                if (tFile.get(x).length() > 0) {
                    Prices.LoadFullMinute(tFile.get(x));
                    if (Prices.Values.size() == 1) {
                        Prices.Date = Prices.Values.get(0).Time;
                        //System.out.println(aDay.Date);
                    }

                }
            }

        }
        if (Prices.Values.size() > 0) {
            Valid = true;
        } else {
            System.out.println("Stock data is insufficient");
        }
    }
    public void LoadArray(String tTicker, String tDate, ArrayList<String> tData) throws IOException, Exception
    {
        //System.out.println("LOADING STOCK: " + tFileName);
        FileSystem aFS = FileSystem.get(new Configuration());

        Path aProcessedPath = new Path("/Chips/Data/Processed/" + tTicker + "_"+tDate + ".txt");
        if (aFS.exists(aProcessedPath)) {
            aFS.delete(aProcessedPath, true);
            System.out.println("existing prices removed");
        }
        if (tData.size() > 180){
            Prices = new StockSegment();
            for (int x = 0; x < tData.size(); x++) {
                //Find the start of a day
                if (tData.get(x).split(",").length == 7) {
                    //System.out.println(aFile.get(x));
                    StockPrice aPrice = new StockPrice(tData.get(x));
                    Prices.Values.add(aPrice);
                }

            }
            if (Prices.Values.size()>100)
            {
                Valid = true;
                //System.out.println("Size correct: " + Prices.Values.size());
            }
            else {
                System.out.println("Size too small for stock: " + Prices.Values.size());
            }
            Prices.Process();
        }
        else
        {
            System.out.println("File length too short: " + tData.size());
            Valid = false;
        }
    }

    public void OutputToFile(String tFilePath, int tStartIndex) throws java.io.IOException
    {

        FileSystem aFS = FileSystem.get(new Configuration());
        Path aPath = new Path("/Chips/Data/Processed/" + tFilePath + ".txt");
        if (aFS.exists(aPath)) {
            aFS.delete(aPath, true);
        }

        BufferedWriter aWriter = new BufferedWriter(new OutputStreamWriter(aFS.create(aPath)));
        if (Valid)
        {
            //System.out.println("Writing");
            aWriter.write(Prices.SaveString(tStartIndex));
        }
        else
        {
            //System.out.println("invalid");
            aWriter.write(" ");
        }
        aWriter.close();
    }

}


class StockSegment {

    public String Date;
    public ArrayList<StockPrice> Values;
    public HashMap<Integer, ArrayList<StockPrice>> Averages;
    public HashMap<Integer, ArrayList<StockPrice>> PercentChanges;
    public static int[] AverageIntervalList = {5,10,15,20,25,30,45,60,90,120,150,180};
    public StockSegment()
    {
        Values = new ArrayList<StockPrice>();
        Averages = new HashMap<Integer, ArrayList<StockPrice>>();
        PercentChanges = new HashMap<Integer, ArrayList<StockPrice>>();
    }
    public StockSegment(boolean tVal)
    {
        Values = new ArrayList<StockPrice>();
        Averages = new HashMap<Integer, ArrayList<StockPrice>>();
        PercentChanges = new HashMap<Integer, ArrayList<StockPrice>>();
        for (int x=0; x<AverageIntervalList.length; x++)
        {
            Averages.put(AverageIntervalList[x], new ArrayList<StockPrice>());
            PercentChanges.put(AverageIntervalList[x], new ArrayList<StockPrice>());
        }
    }

    public void Process()
    {
        //This method will update the average and percent changes lists;
        for (int x=0; x<AverageIntervalList.length; x++)
        {
            ArrayList<ArrayList<StockPrice>> aPair = CalcAveragesFromValues(AverageIntervalList[x]);
            Averages.put(AverageIntervalList[x], aPair.get(0));
            PercentChanges.put(AverageIntervalList[x], aPair.get(1));
        }

    }

    public ArrayList<ArrayList<StockPrice>> CalcAveragesFromValues(int tTimePeriod)
    {
        ArrayList<StockPrice> aAverageIntervalList = new ArrayList<StockPrice>();
        ArrayList<StockPrice> aPercentChangeList = new ArrayList<StockPrice>();
        //double aValueSum
        StockPrice aSum = new StockPrice();
        //aSum.add(Values.get(0));

        for (int x=0; x<Values.size(); x++)
        {

            aSum.add(Values.get(x));
            if (x>=tTimePeriod)
            {
                aSum.minus(Values.get(x-tTimePeriod));
                StockPrice aAverage = aSum.divide(tTimePeriod);
                StockPrice aPercentChange = Values.get(x).divide(aAverage);
                aPercentChange.minus(1);
                aAverage.Time = Values.get(x).Time;
                aPercentChange.Time = Values.get(x).Time;
                aAverageIntervalList.add(aAverage);
                aPercentChangeList.add(aPercentChange);
            }
            else
            {
                aAverageIntervalList.add(new StockPrice());
                aPercentChangeList.add(new StockPrice());
            }

        }

        ArrayList<ArrayList<StockPrice>> aRetList = new ArrayList<ArrayList<StockPrice>>();
        aRetList.add(aAverageIntervalList);
        aRetList.add(aPercentChangeList);
        return aRetList;
    }

    public void LoadFullMinute(String tFullInput)
    {
        String[] aInputList = tFullInput.split("~");
        if (aInputList.length==(1+(AverageIntervalList.length*2))) {
            Values.add(new StockPrice(aInputList[0]));
            for (int x=0; x<AverageIntervalList.length; x++)
            {
                Averages.get(AverageIntervalList[x]).add(new StockPrice(aInputList[x*2+1]));
                PercentChanges.get(AverageIntervalList[x]).add(new StockPrice(aInputList[x*2+2]));
            }
        }
        else
        {
            System.out.println("Loaded minute of wrong size: " + aInputList.length + "\n" + tFullInput);
        }
    }

    public String SaveString(int tStartIndex)
    {
        String aRet = "";
        aRet += Values.get(0) + "~";
        for (int y=0; y<AverageIntervalList.length; y++)
        {
            aRet += Averages.get(AverageIntervalList[y]).get(0) + "~";
            aRet += PercentChanges.get(AverageIntervalList[y]).get(0) + "~";
        }
        aRet += "\n";

        for (int x=tStartIndex; x<Values.size(); x++)
        {
            aRet += Values.get(x) + "~";
            for (int y=0; y<AverageIntervalList.length; y++)
            {
                aRet += Averages.get(AverageIntervalList[y]).get(x) + "~";
                aRet += PercentChanges.get(AverageIntervalList[y]).get(x) + "~";
            }
            aRet += "\n";


        }
        aRet += "\n";
        return aRet;
    }

}

class StockPrice {
    public String Time;
    public double Close;
    public double Open;
    public double High;
    public double Low;
    public double Volume;
    public double FuturePrice;
    StockPrice()
    {
        Time = "Default";
        Close = Open = High = Low = Volume = FuturePrice = 0;
    }

    StockPrice(String tInput)
    {
        //GOOG = COLUMNS=DATE,CLOSE,HIGH,LOW,OPEN,VOLUME
        //MinuteData = DATE,CLOSE,HIGH,LOW,OPEN,VOLUME
        //DailyData = Date,Open,High,Low,Close,Volume
        //Yahoo = Timestamp,close,high,low,open,volume
        //eoddata = Symbol,Date,Open,High,Low,Close,Volume
        String[] aInputList = tInput.split(",");
        switch (aInputList.length)
        {
            case 6:
                Time = aInputList[0];
                Close = Double.parseDouble(aInputList[1]);
                High = Double.parseDouble(aInputList[2]);
                Low = Double.parseDouble(aInputList[3]);
                Open = Double.parseDouble(aInputList[4]);
                Volume = Double.parseDouble(aInputList[5]);
                break;
            case 7:
                Time = aInputList[1];
                Open = Double.parseDouble(aInputList[2]);
                High = Double.parseDouble(aInputList[3]);
                Low = Double.parseDouble(aInputList[4]);
                Close = Double.parseDouble(aInputList[5]);
                Volume = Double.parseDouble(aInputList[6]);
                break;
            default:
                Time = "Default";
                Close = Open = High = Low = Volume = FuturePrice = 0;
                System.out.println("STOCK MINUTE DAY IS WRONG LENGTH: " + tInput);
                break;
        }
    }

    public String toString()
    {
        String aRet = "";
        aRet = Time + "," + Close + ","+ High + "," + Low + ","  + Open + "," + Volume;
        return aRet;
    }

    public void add(StockPrice tOther)
    {
        Close += tOther.Close;
        Open += tOther.Open;
        High += tOther.High;
        Low += tOther.Low;
        Volume += tOther.Volume;
    }

    public void minus(StockPrice tOther)
    {
        Close -= tOther.Close;
        Open -= tOther.Open;
        High -= tOther.High;
        Low -= tOther.Low;
        Volume -= tOther.Volume;
    }
    public void minus(double tOther)
    {
        Close -= tOther;
        Open -= tOther;
        High -= tOther;
        Low -= tOther;
        Volume -= tOther;
    }
    public StockPrice divide(double tDivisor)
    {
        StockPrice aRet = new StockPrice();
        aRet.Close = Close/tDivisor;
        aRet.Open = Open/tDivisor;
        aRet.High = High/tDivisor;
        aRet.Low = Low/tDivisor;
        aRet.Volume = Volume/tDivisor;
        return aRet;
    }
    public StockPrice divide(StockPrice tDivisor)
    {
        StockPrice aRet = new StockPrice();
        aRet.Close = Close/tDivisor.Close;
        aRet.Open = Open/tDivisor.Open;
        aRet.High = High/tDivisor.High;
        aRet.Low = Low/tDivisor.Low;
        aRet.Volume = Volume/tDivisor.Volume;
        return aRet;
    }
}


class StockRating {
    public String Name;
    public double WinnerRating;
    public double LoserRating;
    public double Rating;

    public StockRating(String tName, double tWinner, double tLoser)
    {
        Name = tName;
        WinnerRating = tWinner;
        LoserRating = tLoser;
        Rating = WinnerRating-LoserRating;
    }

    public StockRating(String tName, double tWinner, double tLoser, boolean tAdd)
    {
        Name = tName;
        WinnerRating = tWinner;
        LoserRating = tLoser;
        Rating = WinnerRating+LoserRating;
    }

    public String toString()
    {
        String aRet = "";
        aRet = Name + "," + Rating + "," + WinnerRating + "," + LoserRating + "\n";
        return aRet;
    }
    public void Combine(StockRating tOther)
    {
        WinnerRating += tOther.WinnerRating;
        LoserRating += tOther.LoserRating;
        Rating = WinnerRating+LoserRating;

    }
    public void Evaluate()
    {
        Rating = WinnerRating-LoserRating;
    }

}


/*
    public void LoadRawDaily(String tFileName) throws IOException {
        String aLoadURL = String.format("https://www.google.com/finance/historical?q=%s&histperiod=daily&startdate=Jan+1+2012&output=csv", tFileName);
        System.out.println(aLoadURL);
        URL aAPI = new URL(aLoadURL);
        InputStream aInput = aAPI.openStream();
        BufferedReader aReader = new BufferedReader(new InputStreamReader(aInput));

        String aLine;
        int count = 0;
        aLine = aReader.readLine();
        ArrayList<String> aFile = new ArrayList<String>();
        while (aLine != null) {
            //System.out.println(aLine);
            aFile.add(aLine);
            aLine = aReader.readLine();
            count++;
        }
        aReader.close();
        //Clear the header
        aFile.remove(0);
        Collections.reverse(aFile);

        if (aFile.size()>100)
        {
            Valid = true;
            StockPrice aLastValid = null;
            StockSegment aDay = new StockSegment();
            for (int x = 1; x < aFile.size(); x++) {

                if (!aFile.get(x).contains(",-")) {
                    StockPrice aValue = new StockPrice(aFile.get(x), false);
                    aDay.Values.add(aValue);
                    aLastValid = aValue;
                    if (x == aFile.size() - 1) {
                        //Collections.reverse(aDay.Values);
                        aDay.Date = aDay.Values.get(0).Time;
                        //System.out.println(aDay.Date);
                        aDay.Process();
                        Days.add(aDay);
                        aDay = new StockSegment();
                    }
                }
                else if (aLastValid != null)
                {
                    aDay.Values.add(aLastValid);
                }
            }
        }
        else
        {
            Valid = false;
        }
    }

    public void LoadRawMinute(String tFileName, boolean tIsStream) throws IOException {
        //System.out.println("LOADING RAW: " + tFileName);
        BufferedReader aReader = null;// = new BufferedReader(new FileReader(aStockFile));
        String aLoadURL = String.format("https://www.google.com/finance/getprices?q=%s&i=60&p=", tFileName);
        if (tIsStream) {
            aLoadURL += "1d";
        } else {
            aLoadURL += "30d";
        }
        System.out.println(aLoadURL);
        URL aAPI = new URL(aLoadURL);
        InputStream aInput = aAPI.openStream();
        aReader = new BufferedReader(new InputStreamReader(aInput));

        //BufferedReader aReader = new BufferedReader(new InputStreamReader(aFS.open(aPath)));
        //aReader = new BufferedReader(new FileReader(aStockFile));
        String aLine;
        aLine = aReader.readLine();
        ArrayList<String> aFile = new ArrayList<String>();
        while (aLine != null) {
            aFile.add(aLine);
            aLine = aReader.readLine();
        }
        aReader.close();
        //System.out.println("Lines: " + count);
        if (aFile.size() > 100) {
            Valid = true;
            for (int x = 0; x < aFile.size(); x++) {
                //Find the start of a day
                if (aFile.get(x).charAt(0) == 'a') {
                    StockSegment aDay = new StockSegment();
                    StockPrice aPreviousMinute = new StockPrice(aFile.get(x), true);
                    aDay.Date = aPreviousMinute.Time;
                    aDay.Values.add(aPreviousMinute);
                    x++;
                    //aDay.Values.add(aValue);
                    for (int y = 1; (y <= 390) && (x < aFile.size()); y++) {
                        StockPrice aMinute = new StockPrice(aFile.get(x), true);
                        //Check that the current stock price is the current valid one
                        if ((NumberUtils.isNumber(aMinute.Time)) && (Double.parseDouble(aMinute.Time) == y)) {
                            aDay.Values.add(aMinute);
                            aPreviousMinute = aMinute;
                            if (y != 390) {
                                x++;
                            }
                        } else {
                            //System.out.println("FAKE MINUTE: " + aPreviousMinute);
                            aDay.Values.add(aPreviousMinute);
                        }

                    }
                    aDay.Process();
                    Days.add(aDay);
                }

            }
        }
        else
        {
            Valid = false;
        }

        if (tIsStream) {
            for (int x = 0; x < Days.size(); x++) {
                System.out.println(Days.get(x).Date);
            }
        }
    }
*/