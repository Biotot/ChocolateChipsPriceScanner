import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

/**
 * Created by ken on 3/31/17.
 */
public class Broker implements WritableComparable<Broker> {

    public Text m_GUID;
    public static int m_KeyCount = 65;
    public static int m_MaxHeight = 1000;
    public DoubleWritable[] m_Key = new DoubleWritable[m_KeyCount];
    public DoubleWritable m_PercentReturn;

    /*
    //AverageList = {5,10,15,20,25,30,45,60,90,120,150,180};
    Broker Key Breakdown
    0 = Buy threshold
    1-5 = 5 day average percent change
     */

    public Broker() {
        m_GUID = new Text(java.util.UUID.randomUUID().toString());
        m_PercentReturn = new DoubleWritable(0.0);
        Random aRand = new Random();
        for (int x = 0; x < m_KeyCount; x++) {
            m_Key[x] = new DoubleWritable();
            m_Key[x].set((aRand.nextDouble() * 2 * m_MaxHeight) - m_MaxHeight);
        }
    }

    public Broker(Broker tSeed) {
        m_GUID = new Text(java.util.UUID.randomUUID().toString());
        m_PercentReturn = new DoubleWritable(0.0);
        Random aRand = new Random();
        double aDistance = aRand.nextDouble()*m_MaxHeight;
        for (int x = 0; x < m_KeyCount; x++) {
            double aKeyVal = tSeed.m_Key[x].get() + ((aRand.nextDouble()-0.5)*aDistance);
            aKeyVal = (aKeyVal>m_MaxHeight)? m_MaxHeight : aKeyVal;
            aKeyVal = (aKeyVal<-m_MaxHeight)? -m_MaxHeight : aKeyVal;
            m_Key[x] = new DoubleWritable();
            m_Key[x].set(aKeyVal);
        }
    }

    public Broker(String tBrokerString)
    {
        String[] aSplit = tBrokerString.split(",");
        if (aSplit.length==m_KeyCount+2)
        {
            m_GUID = new Text(aSplit[0]);
            m_PercentReturn = new DoubleWritable(Double.parseDouble(aSplit[1]));
            for (int x=0; x<m_KeyCount; x++)
            {
                m_Key[x] = new DoubleWritable(Double.parseDouble(aSplit[x+2]));
            }
        }
        else
        {
            m_GUID = new Text("I AM BROKEN");
            m_PercentReturn = new DoubleWritable(0);
            for (int x=0; x<m_KeyCount; x++)
            {
                m_Key[x] = new DoubleWritable(0);
            }
        }
    }


    public int compareTo(Broker o) {
        return this.m_GUID.compareTo(o.m_GUID);
    }

    @Override
    public boolean equals(Object o) {
        return this.m_GUID.toString().equals(((Broker) o).m_GUID.toString());
    }



    public void write(DataOutput out) throws IOException {
        //aInt.write(out);
        m_GUID.write(out);
        m_PercentReturn.write(out);
        for (int x=0; x<m_KeyCount; x++)
        {
            m_Key[x].write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        m_GUID.readFields(in);
        m_PercentReturn.readFields(in);
        for (int x=0; x<m_KeyCount; x++)
        {
            this.m_Key[x].readFields(in);
        }
    }
    public String toString()
    {
        String aRet = m_GUID.toString();
        /*for (int x=0; x<m_KeyCount; x++)
        {
            //aRet += " " + m_Key[x];
        }*/
        return aRet;
    }
    public String SaveString()
    {
        String aRet = m_GUID.toString();
        aRet += ","+m_PercentReturn.get();
        for (int x=0; x<m_KeyCount; x++)
        {
            aRet += "," + m_Key[x].get();
        }
        return aRet;
    }


    public double RunFull(Stock tStockTarget) {
        double aRet = 0;
        double aBuyThreshhold = m_Key[0].get() * m_Key[1].get();
        int aBuyDays = 0;

        StockSegment aDay = tStockTarget.Prices;
        double aInvestment = 0;
        double aSellValue = 0;
        for (int y = 1; y < aDay.Values.size() - 1; y++) {

            boolean aBuy = Evaluate(aBuyThreshhold, aDay.PercentChanges, y);
            //THIS IS A BUY SIGNAL
            if (aBuy) {
                aInvestment += aDay.Values.get(y).Close;
                aSellValue += aDay.Values.get(y + 1).Close;
            }
        }

        if (aInvestment != 0) {
            aRet += aSellValue / aInvestment;
            aBuyDays++;
        }

        if (aBuyDays > 0) {
            aRet = aRet / aBuyDays;
        } else {
            aRet = 1;
        }

        return aRet;
    }

    boolean Evaluate(HashMap<Integer, ArrayList<StockPrice>> tMinuteMap, int tIndex)
    {
        double aBuyThreshhold = m_Key[0].get() * m_Key[1].get();
        return Evaluate(aBuyThreshhold, tMinuteMap, tIndex);
    }

    boolean Evaluate(double tBuyThreshold, HashMap<Integer, ArrayList<StockPrice>> tMinuteMap, int tIndex)
    {
        double aScore = 0;
        int aIndex = 2;

        for(int z=0; z<StockSegment.AverageIntervalList.length; z++)
        {
            StockPrice aPercentChange = tMinuteMap.get(StockSegment.AverageIntervalList[z]).get(tIndex);
            aScore += aPercentChange.Close*m_Key[aIndex+0].get();
            aScore += aPercentChange.Open*m_Key[aIndex+1].get();
            aScore += aPercentChange.High*m_Key[aIndex+2].get();
            aScore += aPercentChange.Low*m_Key[aIndex+3].get();
            aScore += aPercentChange.Volume*m_Key[aIndex+4].get();
            aIndex+=5;
        }
        return (aScore > tBuyThreshold);
    }
}