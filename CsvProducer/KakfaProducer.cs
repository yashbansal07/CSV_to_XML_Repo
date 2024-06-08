using Confluent.Kafka;
using System.Diagnostics;

namespace CsvProducer
{
    internal class KakfaProducer
    {
        private readonly string _uri;

        private readonly string _producerTopic;
        private readonly string _triggerConsumerTopic;
        //private readonly EnvConfigs _envConfigs;

        private Stopwatch _stopwatch;

        IProducer<Null, string> _producerObj;

        public KakfaProducer(EnvConfigs envConfigs)
        {
            _stopwatch = new Stopwatch();
            _stopwatch.Start();
            //_envConfigs = envConfigs;
            _uri = envConfigs.KafkaServerURL;

            _producerTopic = envConfigs.ProducerTopicName;

            _triggerConsumerTopic = envConfigs.TriggerConsumerTopic;

            var producerConfig = new ProducerConfig { BootstrapServers = _uri.ToString() };
            _producerObj = new ProducerBuilder<Null, string>(producerConfig).Build();

            Console.WriteLine("In KakfaProducer | Kafka Connected to Server: {0}", _uri.ToString());
        }


        Action<DeliveryReport<Null, string>> producerHandler = r =>
            Console.WriteLine(!r.Error.IsError
                ? $"Delivered message {r.Message.Value} to {r.TopicPartitionOffset}"
                : $"[Kafka Error] In ProduceMessageToKafka | Delivery Error: {r.Error.Reason}");

        public void ProduceMessageToKafka(string message)
        {
            //Console.WriteLine("[Started] In ProduceMessageToKafka | Producing Message, kafka Topic: {0} Message: {1}", _producerTopic, message);

            _producerObj.Produce(_producerTopic, new Message<Null, string> { Value = message }, producerHandler);
            /*var result = _producerObj.ProduceAsync(_producerTopic, new Message<Null, string> { Value = message }).Result;
            Console.WriteLine($"In ProduceMessageTo{_producerTopic}Topic | Message sent on Partition: {result.Partition} with Offset: {result.Offset}");*/

            //Console.WriteLine("[DONE] In ProduceMessageToKafka | Message Produced!!");
        }
        public void ProduceMessageToTriggerConsumerTopic(string message = "Start")
        {
            //Console.WriteLine("[Started] In ProduceMessageToTriggerConsumerTopic | Producing Message, kafka Topic: {0} Message: {1}", _triggerConsumerTopic, message);

            _producerObj.Produce(_triggerConsumerTopic, new Message<Null, string> { Value = message }, producerHandler);
            /*var result = _producerObj.ProduceAsync(_triggerConsumerTopic, new Message<Null, string> { Value = message }).Result;
            Console.WriteLine($"In ProduceMessageTo{_triggerConsumerTopic}Topic | Message sent on Partition: {result.Partition} with Offset: {result.Offset}");*/

            //Console.WriteLine("[DONE] In ProduceMessageToTriggerConsumerTopic | Message Produced!!");
        }

        public void WaitForAllOutStandingProduceRequest()
        {
            _producerObj.Flush();
        }
    }
}
