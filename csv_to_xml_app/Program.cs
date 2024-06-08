using csv_to_xml_app;
using csv_to_xml_app.Services;
using Microsoft.Extensions.Configuration;

public class Program
{
    //D:\Dummy document-3-Floating  hyperlinks.pdf
    //D:\Combined file with appendix 7.1.pdf

    public static void Main(string[] args)
    {
        FileStream ostrm;
        StreamWriter writer;
        TextWriter oldOut = Console.Out;
        /*try
        {
            ostrm = new FileStream("D:\\Redirect.txt", FileMode.OpenOrCreate, FileAccess.Write);
            writer = new StreamWriter(ostrm);
        }
        catch (Exception e)
        {
            Console.WriteLine("Cannot open Redirect.txt for writing");
            Console.WriteLine(e.Message);
            return;
        }*/
        ostrm = new FileStream("D:\\Redirect.txt", FileMode.OpenOrCreate, FileAccess.Write);
        writer = new StreamWriter(ostrm);

        //Console.SetOut(writer);
        
        Console.WriteLine("Csv_To_Xml Service!!");

        var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile($"appsettings.json");

        var configuration = builder.Build();

        EnvConfigs envConfigObj = EnvConfigs.GetEnvConfigObject(configuration);

        ParseFromCsv obj = new ParseFromCsv(envConfigObj);

        obj.ReadFromCsv("D:\\Visual Studio\\Source3000Lines.csv");

        /*Console.SetOut(oldOut);
        writer.Close();
        ostrm.Close();
        Console.WriteLine("Done");*/
        return;
    }

    /*private static void TriggerConfluentKafkaConsumer(EnvConfigs envConfigObj)
    {
        ConfluentKafka kafkaObj = new ConfluentKafka(envConfigObj);
        Console.WriteLine("Kafka Connected at Hosted URL: {0}", envConfigObj.KafkaServerURL);

        kafkaObj.ConsumeMessage();
    }*/

}