using csv_to_xml_app.Constants;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace csv_to_xml_app.Services
{
    public class ParseFromCsv
    {
        private KafkaService _kafkaObj = null;
        private EnvConfigs _envConfigObj = null;

        private int _indexOfItemNumberCol;
        private int _indexOfMsrpCol;

        private int LinesCountToReadFromCSV = 100;

        private List<string> _dataFromCsv = new List<string>();

        public ParseFromCsv(EnvConfigs envConfigObj)
        {
            _envConfigObj = envConfigObj;
            _kafkaObj = new KafkaService(envConfigObj);

            //_ = _kafkaObj.TriggerConsumerToContinuousPollAsync();
        }

        private Stopwatch sw = new Stopwatch();

        public void ReadFromCsv(string csvFilePath)
        {
            //Task.Run(() => _kafkaObj.TriggerConsumerToFormXmlAsync());
            //_kafkaObj.TriggerConsumerToFormXmlAsync();

            var consumerTask = Task.Run(() => _kafkaObj.TriggerConsumerToContinuousPollAsync());

            sw.Start();
            Console.WriteLine("In ReadFromCsv | Entered csvFilePath: {0}", csvFilePath);
            var reader = new StreamReader(csvFilePath);

            //return;

            //_kafkaObj.ProduceMessageToTriggerConsumerTopic();

            //XmlToJson();

            //PrepareAndSaveXml(null);
            //return;

            //var csv = reader.ReadToEnd();

            //var lines = csv.Split('\n', StringSplitOptions.RemoveEmptyEntries).ToList();

            var firstHeaderLine = reader.ReadLine();

            List<string?> headerList = firstHeaderLine.Split(',').ToList();

            _indexOfItemNumberCol = findIndexOfColumn(headerList, HeaderConstants.ItemNumber);
            _indexOfMsrpCol = findIndexOfColumn(headerList, HeaderConstants.MSRP);


            ReadAndPushLinesToKafka(reader);
            //Push3KLinesToKafka(lines);

            //var canStartXml = _kafkaObj.CheckToStartOnProcessingOrNot();

            /*if(true)
            {
                var dataFromCsv = _kafkaObj.TriggerConsumerToFormXml();

                PrepareAndSaveXml(_dataFromCsv);
            }*/

            //This is imp, otherwise the program will exit, without Completing the XML task.
            Task.WaitAll(consumerTask);
        }

        private void ReadAndPushLinesToKafka(StreamReader reader)
        {
            int count = 0;
            Stopwatch s = Stopwatch.StartNew();
            while (!reader.EndOfStream && count < 3000)
            {
                var csvRow = reader.ReadLine();
                count++;

                var values = csvRow.Split(',').ToList();

                var msg = values[_indexOfItemNumberCol] + "," + values[_indexOfMsrpCol];

                _dataFromCsv.Add(msg);

                _kafkaObj.ProduceMessageToKafka(msg);

                if(count % ApplicationConstant.LinesCountToReadFromCSV == 0)
                {
                    _kafkaObj.ProduceMessageToTriggerConsumerTopic();
                    //Console.WriteLine("In ReadAndPushLinesToKafka | After Trigging Event the time is {0}", s.ElapsedMilliseconds);
                    Console.WriteLine("In ReadAndPushLinesToKafka | After Trigging Event the time is {0}", sw.ElapsedMilliseconds);
                }
            }
            Console.WriteLine("In ReadAndPushLinesToKafka | Total Rows Read - {0}", count);
            //_kafkaObj.ProduceMessageToTriggerConsumerTopic();
        }

        private void Push3KLinesToKafka(List<string> lines)
        {
            int count = 1;

            while(count < lines.Count && count <= 3000)
            {
                var csvRow = lines[count];
                count++;

                var values = csvRow.Split(',').ToList();

                var msg = values[_indexOfItemNumberCol] + "," + values[_indexOfMsrpCol];

                _dataFromCsv.Add(msg);

                //_kafkaObj.ProduceMessageToKafka(msg);
            }

            //_kafkaObj.ProduceMessageToTriggerConsumerTopic();
        }

        public int findIndexOfColumn(List<string?> headerList, string columnName)
        {
            for(int i = 0; i < headerList.Count; i++)
            {
                if (headerList[i] == columnName)
                    return i;
            }
            return -1;
        }

        private void PrepareAndSaveXml(List<string> dataFromCsv)
        {
            var finalJsonString = string.Empty;
            foreach(var row in dataFromCsv)
            {
                var data = row.Split(',');

                var productId = data[0];
                var amount = data[1];

                string s = @"{
                            '@product-id': ' " + productId + @" ' ,
                            'amount':  {
                               '@quantity': '1',
                                '#text': ' " + amount + @" '
                                }
                            }";

                finalJsonString += s + ',';
            }

            string json = @"{
                    '?xml': { '@version': '1.0',  '@encoding': 'UTF-8'},
                    'pricebooks': {
                          '@xmlns': 'http://www.demandware.com/xml/impex/pricebook/2006-10-31',
                          'pricebook':{
                              'header': {
                                    '@pricebook-id': 'p8-uk-gb-gbp-list-prices',
                                    'currency': 'GBP',
                                    'display-name': {
                                          '@xml:lang': 'x-default',
                                          '#text': 'GB GBP list prices'
                                      },
                                    'online-flag': 'true'
                                  },
                              'price-tables':{
                                      'price-table':[ " + finalJsonString + @" ]
                                      }
                                    }
                              }}";


            XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json);
            doc.Save("D:\\json.xml");
        }
    }
}
