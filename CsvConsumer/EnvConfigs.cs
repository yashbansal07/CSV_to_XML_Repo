using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvConsumer
{
    public class EnvConfigs
    {
        private static EnvConfigs _env = null;
        public static IConfigurationRoot _config = null;

        private static readonly object _lockObjectForInstanceCreation = new object();

        public string KafkaServerURL = _config["KafkaServerURL"];

        public string ProducerTopicName = _config["ProducerTopicName"];

        public string TriggerConsumerTopic = _config["TriggerConsumerTopicName"];


        public static EnvConfigs GetEnvConfigObject(IConfigurationRoot configuration)
        {
            lock (_lockObjectForInstanceCreation)
            {
                if (_env == null)
                {
                    _config = configuration;
                    _env = new EnvConfigs();
                }
                return _env;
            }
        }
    }
}
