using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json;
using Polly;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace exponential_backoff_aws
{
    class Program
    {
        static readonly IEnumerable<string> ANIMALS = JsonConvert.DeserializeObject<IEnumerable<string>>(File.ReadAllText("Animals.json"));

        public static void Main(string[] args)
        {
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(new AmazonDynamoDBConfig()
            {
                MaxErrorRetry = 0
            });
            string tableName = "AnimalsInventory";
            CreateTable(client, tableName).Wait();
            LoadAnimals(client, tableName);
            Console.WriteLine("> Press any key to delete table and quit.");
            Console.ReadKey();
            DeleteTable(client, tableName).Wait();
        }

        private static async Task CreateTable(AmazonDynamoDBClient client, string tableName)
        {
            CreateTableRequest request = new CreateTableRequest
            {
                TableName = tableName,
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Id",
                        AttributeType = "S"
                    },
                    new AttributeDefinition
                    {
                        AttributeName = "Type",
                        AttributeType = "S"
                    }
                },
                KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement
                    {
                      AttributeName = "Id",
                      KeyType = "HASH"
                    },
                    new KeySchemaElement
                    {
                      AttributeName = "Type",
                      KeyType = "RANGE"
                    },
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 1,
                    WriteCapacityUnits = 2
                },
            };

            await client.CreateTableAsync(request);
            await Policy
                .HandleResult<DescribeTableResponse>(resp =>
                {

                    Console.WriteLine("Waiting for table to become active.");
                    return resp.Table.TableStatus != "ACTIVE";
                })
                .WaitAndRetryAsync(12, i => TimeSpan.FromMilliseconds(20 * Math.Pow(2, i)).AddJitter(400, 900))
                .ExecuteAsync(async () => await client.DescribeTableAsync(tableName));
            Console.WriteLine($"Table {tableName} was created.");
        }

        private static void LoadAnimals(AmazonDynamoDBClient client, string tableName)
        {
            int insertCount = ANIMALS.Count();
            ManualResetEvent signal = new ManualResetEvent(false);
            foreach (string a in ANIMALS)
            {
                ThreadPool.QueueUserWorkItem(async (state) =>
                {
                    PutItemRequest req = new PutItemRequest
                    {
                        TableName = tableName,
                        Item = new Dictionary<string, AttributeValue>
                        {
                            { "Id", new AttributeValue { S = Guid.NewGuid().ToString() }},
                            { "Type", new AttributeValue { S = a }},
                        }
                    };
                    try
                    {
                        await Policy
                            .Handle<Exception>(e =>
                            {
                                Console.WriteLine(e.Message);
                                return true;
                            })
                            .WaitAndRetryAsync(12, i =>
                            {
                                Console.WriteLine($"Retry attempt {i} for animial: {a}.");
                                // Waits a max of about 1.4 min
                                return TimeSpan.FromMilliseconds(20 * Math.Pow(2, i)).AddJitter(200, 600);
                            })
                            .ExecuteAsync(async () => await client.PutItemAsync(req));
                    }
                    finally
                    {
                        Interlocked.Decrement(ref insertCount);
                        if (insertCount == 0)
                        {
                            signal.Set();
                        }
                    }
                });
            }

            signal.WaitOne();
        }

        private static async Task DeleteTable(AmazonDynamoDBClient client, string tableName)
        {
            await client.DeleteTableAsync(tableName);
            Console.WriteLine($"Table {tableName} was deleted.");
        }
    }

    public static class Extensions
    {
        public static TimeSpan AddJitter(this TimeSpan ts, int msLower, int msHigher)
        {
            return ts.Add(TimeSpan.FromMilliseconds(new Random().Next(msLower, msHigher)));
        }
    }
}
