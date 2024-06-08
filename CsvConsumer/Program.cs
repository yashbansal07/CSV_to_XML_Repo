using CsvConsumer;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

public class Program
{
    /*private KakfaConsumer _kafkaObj = null;
    private EnvConfigs _envConfigObj = null;

    public Program(EnvConfigs envConfigObj)
    {
        _kafkaObj = new KakfaConsumer(envConfigObj);
    }*/

    public static void Main(string[] args)
    {
        Console.WriteLine("Csv_To_Xml Consumer Service!!");

        var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile($"appsettings.json");

        var configuration = builder.Build();

        EnvConfigs envConfigObj = EnvConfigs.GetEnvConfigObject(configuration);

        var kafkaObj = new KakfaConsumer(envConfigObj);

        //kafkaObj.TriggerConsumerToContinuousPollAsync().GetAwaiter().GetResult();
        kafkaObj.TriggerConsumerToFormXmlAsync();

        return;
    }



}