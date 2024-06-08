using CsvProducer;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

public class Program
{
    private KakfaProducer _kafkaObj = null;
    //private EnvConfigs _envConfigObj = null;

    private int _indexOfItemNumberCol;
    private int _indexOfMsrpCol;

    private List<string> _dataFromCsv = new List<string>();

    public Program(EnvConfigs envConfigObj)
    {
        _kafkaObj = new KakfaProducer(envConfigObj);
    }

    public static void Main(string[] args)
    {
        Console.WriteLine("Csv_To_Xml Producer Service!!");

        var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile($"appsettings.json");

        var configuration = builder.Build();

        EnvConfigs envConfigObj = EnvConfigs.GetEnvConfigObject(configuration);

        var obj = new Program(envConfigObj);

        obj.ReadFromCsv("D:\\Visual Studio\\Source50000Lines.csv");

        return;
    }

    public void ReadFromCsv(string csvFilePath)
    {
        Console.WriteLine("In ReadFromCsv | Entered csvFilePath: {0}", csvFilePath);

        var reader = new StreamReader(csvFilePath);

        var firstHeaderLine = reader.ReadLine();

        List<string?> headerList = firstHeaderLine.Split(',').ToList();

        _indexOfItemNumberCol = this.findIndexOfColumn(headerList, AppConstants.ItemNumber);
        _indexOfMsrpCol = findIndexOfColumn(headerList, AppConstants.MSRP);

        //_kafkaObj.ProduceMessageToTriggerConsumerTopic();

        ReadAndPushLinesToKafka(reader);
    }
    public int findIndexOfColumn(List<string?> headerList, string columnName)
    {
        for (int i = 0; i < headerList.Count; i++)
        {
            if (headerList[i] == columnName)
                return i;
        }
        return -1;
    }

    private void ReadAndPushLinesToKafka(StreamReader reader)
    {
        int count = 0;
        Console.WriteLine("====== PRODUCER STARTS ======");
        Stopwatch s = Stopwatch.StartNew();
        while (!reader.EndOfStream && count < AppConstants.TotallLinesToProcessInCSV)
        {
            var csvRow = reader.ReadLine();
            count++;

            var values = csvRow.Split(',').ToList();

            var msg = values[_indexOfItemNumberCol] + "," + values[_indexOfMsrpCol];

            _dataFromCsv.Add(msg);

            _kafkaObj.ProduceMessageToKafka(msg);

            if (count % AppConstants.LinesCountToReadFromCSV == 0)
            {
                _kafkaObj.ProduceMessageToTriggerConsumerTopic();
                _kafkaObj.WaitForAllOutStandingProduceRequest();
                Console.WriteLine("In ReadAndPushLinesToKafka | After Producing, The time is {0}", s.ElapsedMilliseconds);
                Console.WriteLine("In ReadAndPushLinesToKafka | Total count is {0}", count);
            }
        }
        Console.WriteLine("In ReadAndPushLinesToKafka | Total Rows Read - {0}", count);
        _kafkaObj.ProduceMessageToTriggerConsumerTopic();
        _kafkaObj.WaitForAllOutStandingProduceRequest();

    }

}