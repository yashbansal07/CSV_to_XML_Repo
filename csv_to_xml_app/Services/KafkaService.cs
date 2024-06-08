using Confluent.Kafka;
using csv_to_xml_app.Constants;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace csv_to_xml_app.Services
{
    public class KafkaService
    {
        //private readonly Uri _uri;

        private readonly string _uri;

        private readonly string _producerTopic;
        private readonly string _triggerConsumerTopic;
        private readonly EnvConfigs _envConfigs;

        private Stopwatch _stopwatch;

        IProducer<Null, string> _producerObj;

        public KafkaService(EnvConfigs envConfigs)
        {
            _stopwatch = new Stopwatch();
            _stopwatch.Start();
            _envConfigs = envConfigs;
            //_uri = new Uri(envConfigs.KafkaServerURL, UriKind.Absolute);
            _uri = envConfigs.KafkaServerURL;

            _producerTopic = envConfigs.ProducerTopicName;

            _triggerConsumerTopic = envConfigs.TriggerConsumerTopic;

            var producerConfig = new ProducerConfig { BootstrapServers = _uri.ToString() };
            _producerObj = new ProducerBuilder<Null, string>(producerConfig).Build();
        }

        
        Action<DeliveryReport<Null, string>> producerHandler = r =>
            Console.WriteLine(!r.Error.IsError
                ? $"Delivered message {r.Message.Value} to {r.TopicPartitionOffset}"
                : $"[Kafka Error] In ProduceMessageToKafka | Delivery Error: {r.Error.Reason}");

        public void ProduceMessageToKafka(string message)
        {
            //Console.WriteLine("[Started] In ProduceMessageToKafka | Producing Message, kafka Topic: {0} Message: {1}", _producerTopic, message);

            _producerObj.Produce(_producerTopic, new Message<Null, string> { Value = message }/*, producerHandler*/);

            //Console.WriteLine("[DONE] In ProduceMessageToKafka | Message Produced!!");
        }
        public void ProduceMessageToTriggerConsumerTopic(string message = "Start")
        {
            //Console.WriteLine("[Started] In ProduceMessageToTriggerConsumerTopic | Producing Message, kafka Topic: {0} Message: {1}", _triggerConsumerTopic, message);

            _producerObj.Produce(_triggerConsumerTopic, new Message<Null, string> { Value = message }/*, producerHandler*/);

            //Console.WriteLine("[DONE] In ProduceMessageToTriggerConsumerTopic | Message Produced!!");
        }

        /*
         * Keep On Polling for Continuous proccessing
         */
        public async Task TriggerConsumerToContinuousPollAsync()
        {
            var conf = new ConsumerConfig
            {
                GroupId = "tigger-consumer-group",
                BootstrapServers = _uri.ToString(),

                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            Stopwatch S = Stopwatch.StartNew();

            Console.WriteLine("====== CONSUMER STARTS ======");
            using (var _consumerObj = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                _consumerObj.Subscribe(_triggerConsumerTopic);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        S.Start();
                        Console.WriteLine("In TriggerConsumerToContinuousPollAsync | Polling Starts on Topic: {0}", _triggerConsumerTopic);
                        var consumedResult = _consumerObj.Consume(cts.Token);
                        //Console.WriteLine($"In TriggerConsumerToContinuousPollAsync | Consumed message: '{consumedResult.Message.Value}' TopicPartitionOffset: '{consumedResult.TopicPartitionOffset}'.");

                        var msgString = consumedResult.Message.Value;

                        //Console.WriteLine("In TriggerConsumerToContinuousPollAsync | Committing Consumed Message with TopicPartition Offset: {0}", consumedResult.TopicPartitionOffset);
                        _consumerObj.Commit(consumedResult);
                        //Console.WriteLine("In TriggerConsumerToContinuousPollAsync | Committed Consumed Message with TopicPartition Offset: {0}", consumedResult.TopicPartitionOffset);

                        await Task.Run(() => TriggerConsumerToFormXmlAsync());

                        var files = Directory.GetFiles("D:\\XML");
                        if(files.Length > 0)
                        {
                            Console.WriteLine("In TriggerConsumerToContinuousPollAsync | File Exist!!");
                            Console.WriteLine("In TriggerConsumerToContinuousPollAsync | Time is {0}", S.ElapsedMilliseconds);
                        }
                        else
                        {
                            Console.WriteLine("In TriggerConsumerToContinuousPollAsync | File doesn't Exist!!");
                        }

                        //await TriggerConsumerToFormXmlAsync();
                        //break;
                        //return true;
                    }
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"[Consumer Exception] Error occured: {e.Error.Reason}");
                }
                catch (OperationCanceledException ex)
                {
                    Console.WriteLine($"[Consumer OperationCanceled] Message: {ex.Message}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("In ConfluentKafka @ ConsumeMessage | Exception Occurred, Ex Message: {0}", ex.Message);
                }

                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                _consumerObj.Close();
                //return false;
            }
        }

        public void TriggerConsumerToFormXmlAsync()
        {
            List<string> messages = new List<string>();

            Console.WriteLine("In TriggerConsumerToFormXml | Entered!!");
            var conf = new ConsumerConfig
            {
                GroupId = "csv-consumer-group",
                BootstrapServers = _uri.ToString(),

                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            Stopwatch s = Stopwatch.StartNew();


            //PrepareAndSaveXMLService obj = new PrepareAndSaveXMLService();

            using (var _consumerObj = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                _consumerObj.Subscribe(_producerTopic);
                Console.WriteLine("In TriggerConsumerToFormXml | Consumer subscribe to Topic: {0}", _producerTopic);

                bool isContinue = true;

                try
                {
                    while (isContinue)
                    {
                        try
                        {
                            //Console.WriteLine("In TriggerConsumerToFormXml | Polling Starts on Topic: {0}", _producerTopic);
                            var consumedResult = _consumerObj.Consume(10000); // poll for 2000 miliseconds ~ 2 seconds

                            if(consumedResult == null || consumedResult.IsPartitionEOF)
                            {
                                Console.WriteLine($"In TriggerConsumerToFormXml | Cosumer EOF Reached, consumedResult: '{consumedResult}'.");
                                isContinue = false;
                                break;
                            }
                            //Console.WriteLine($"In TriggerConsumerToFormXml | Consumed message: '{consumedResult.Message.Value}' TopicPartitionOffset: '{consumedResult.TopicPartitionOffset}'.");

                            var msgString = consumedResult.Message.Value;

                            //Console.WriteLine("In TriggerConsumerToFormXml | Committing Consumed Message with TopicPartition Offset: {0}", consumedResult.TopicPartitionOffset);
                            _consumerObj.Commit(consumedResult);
                            //Console.WriteLine("In TriggerConsumerToFormXml | Committed Consumed Message with TopicPartition Offset: {0}", consumedResult.TopicPartitionOffset);

                            messages.Add(msgString);

                            if(messages.Count == ApplicationConstant.LinesCountToReadFromCSV) {
                                Console.WriteLine("In TriggerConsumerToFormXml | Message Count Reached, Exiting Consumer!!");
                                /*obj.PrepareAndSaveXML(messages);
                                messages.Clear();*/
                                isContinue = false;
                                break;
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"[Consumer Exception] In TriggerConsumerToFormXml | Error occured: {e.Error.Reason}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("In TriggerConsumerToFormXml | Exception Occurred, Ex Message: {0}", ex.Message);
                            if (ex is OperationCanceledException)
                                throw ex;
                        }
                    }
                }
                catch (OperationCanceledException ex)
                {
                    Console.WriteLine($"[ConsumerOperationCanceled] In TriggerConsumerToFormXml | Cancelled Message: {ex.Message}");
                }

                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                _consumerObj.Close();
            }

            if (messages.Count > 0)
            {
                Console.WriteLine("In TriggerConsumerToFormXml | Preapring XML for {0} Messages", messages.Count);
                PrepareAndSaveXMLService obj = new PrepareAndSaveXMLService();
                obj.PrepareAndSaveXML(messages);
                Console.WriteLine("In TriggerConsumerToFormXml | XML Prepared!!");
                Console.WriteLine("In TriggerConsumerToFormXml | Consumer Time {0}", s.ElapsedMilliseconds);
            }

            //return messages;
        }
    }
}
