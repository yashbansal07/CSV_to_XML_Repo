using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace csv_to_xml_app.Services
{
    public class PrepareAndSaveXMLService
    {
        /*private readonly string _SaveFilePath = string.Empty;

        public PrepareAndSaveXMLService()
        {
            _SaveFilePath = @"D:\Json_" + new Random().Next() + ".xml";
        }*/

        public void PrepareAndSaveXML(List<string> dataFromCsv)
        {
            Console.WriteLine("In PrepareAndSaveXML | Entered for {0} CsvRows!!", dataFromCsv.Count);
            var arrayOfJsonObjectString = GetArrayOfJsonObject(dataFromCsv);

            var finalXmlJson = GetFinalXMLJson(arrayOfJsonObjectString);

            XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode (finalXmlJson);
            Console.WriteLine("In PrepareAndSaveXML | XmlDocument Preapred!!");

            var saveFilePath = GetSaveFilePath();

            if (doc != null)
            {
                doc.Save(saveFilePath);
                Console.WriteLine("In PrepareAndSaveXML | XmlDocument Saved to {0}!!", saveFilePath);
            }
        }

        private string GetSaveFilePath()
        {
            return @"D:\XML\Json_" + new Random().Next() + ".xml";
        }

        private string GetFinalXMLJson(string arrayOfJsonObjectString)
        {
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
                                      'price-table':[ " + arrayOfJsonObjectString + @" ]
                                      }
                                    }
                              }}";
            return json;
        }

        public string GetArrayOfJsonObject(List<string> dataFromCsv)
        {
            var finalJsonString = string.Empty;
            foreach (var row in dataFromCsv)
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
            return finalJsonString;
        }


        // Helper Method
        public void XmlToJson()
        {
            string xml = @"<?xml version=""1.0"" encoding=""UTF-8""?>
                            <pricebooks xmlns=""http://www.demandware.com/xml/impex/pricebook/2006-10-31"">
                              <pricebook>
                                <header pricebook-id=""p8-uk-gb-gbp-list-prices"">
                                  <currency>GBP</currency>
                                  <display-name xml:lang=""x-default"">GB GBP list prices</display-name>
                                  <online-flag>true</online-flag>
                                </header>
                                <price-tables>
                                  <price-table product-id=""22157729608"">
                                    <amount quantity=""1"">139.00</amount>
                                  </price-table>
                                </price-tables>
                              </pricebook>
                            </pricebooks>";

            XmlDocument doc = new XmlDocument();
            doc.LoadXml(xml);

            string jsonText = JsonConvert.SerializeXmlNode(doc, Newtonsoft.Json.Formatting.Indented);
        }

    }
}
