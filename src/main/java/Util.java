import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

/**
 * Created by ken on 3/31/17.
 */
public class Util {





    public static void CreateNewBrokerList(String tFileName) throws Exception
    {
        System.out.println("Creating new BrokerList");
        int aBrokerCount = 10000;
        FileSystem aFS = FileSystem.get(new Configuration());
        Path aPath = new Path("/Chips/Data/Brokers/New"+tFileName+"List.txt");
        Path aMasterBrokerPath = new Path("/Chips/Data/Brokers/"+tFileName+"Master.txt");
        Path aFlagPath = new Path("/Chips/Flags/Refresh"+tFileName+"Master");
        ArrayList<Broker> aBrokerList = new ArrayList<Broker>();
        ArrayList<Broker> aMasterList = new ArrayList<Broker>();

        if (aFS.exists(aPath)) {
            aFS.delete(aPath, true);
        }

        if (aFS.exists(aMasterBrokerPath)) {
            BufferedReader aMasterBrokerReader = new BufferedReader(new InputStreamReader(aFS.open(aMasterBrokerPath)));
            String aLine = aMasterBrokerReader.readLine();
            while (aLine!=null) {
                aMasterList.add(new Broker(aLine));
                aLine = aMasterBrokerReader.readLine();
            }
            aMasterBrokerReader.close();
        }
        else
        {
            for (int x=0; x<2000; x++)
            {
                aMasterList.add(new Broker());
            }
        }

        if (aFS.exists(aFlagPath)) {
            System.out.println("Including Master List");
            for (Broker aBroker : aMasterList)
            {
                aBrokerList.add(aBroker);
            }
        }


        BufferedWriter aWriter = new BufferedWriter(new OutputStreamWriter(aFS.create(aPath)));
        for (int x=aBrokerList.size(); aBrokerList.size()<aBrokerCount; x++)
        {
            aBrokerList.add(new Broker(aMasterList.get(x%2000)));
        }

        for (Broker aBroker : aBrokerList)
        {
            aWriter.write(aBroker.SaveString() + "\n");
        }

        aWriter.close();
        System.out.println("BrokerList Created");
    }

}
