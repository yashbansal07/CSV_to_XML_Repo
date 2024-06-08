using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace csv_to_xml_app
{
    public class Producer
    {
        private readonly string _uri;

        private readonly string _producerTopic;
        private readonly string _triggerConsumerTopic;
        private readonly EnvConfigs _envConfigs;

        private Stopwatch _stopwatch;

        IProducer<Null, string> _producerObj;

        public Producer(EnvConfigs envConfigs)
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
    }
}
