using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LiteDB;

namespace TestPerfLiteDB
{
    static class Helper
    {
        public static void Run(this ITest test, string name, Action action)
        {
            var sw = new Stopwatch();

            sw.Start();
            action();
            sw.Stop();

            var time = sw.ElapsedMilliseconds.ToString().PadLeft(5, ' ');
            var seg = Math.Round(test.Count * test.Times / sw.Elapsed.TotalSeconds).ToString().PadLeft(8, ' ');

            Console.WriteLine(name.PadRight(15, ' ') + ": " + 
                time + " ms - " + 
                seg + " records/second");
        }

        public static IEnumerable<BsonDocument> GetDocs(int count, int times = 0)
        {
            for(var i = 0; i < count; i++)
            {
                var doc = new BsonDocument();
                doc["_id"] = i + count * times;
                doc["name"] = Guid.NewGuid().ToString();
                doc["lorem"] = LoremIpsum(3, 5, 2, 3, 3);
                yield return doc;
            }
        }

        public static string LoremIpsum(int minWords, int maxWords,
            int minSentences, int maxSentences,
            int numParagraphs)
        {
            var words = new[] { "lorem", "ipsum", "dolor", "sit", "amet", "consectetuer",
                "adipiscing", "elit", "sed", "diam", "nonummy", "nibh", "euismod",
                "tincidunt", "ut", "laoreet", "dolore", "magna", "aliquam", "erat" };

            var rand = new Random(DateTime.Now.Millisecond);
            var numSentences = rand.Next(maxSentences - minSentences) + minSentences + 1;
            var numWords = rand.Next(maxWords - minWords) + minWords + 1;

            var result = new StringBuilder();

            for (int p = 0; p < numParagraphs; p++)
            {
                for (int s = 0; s < numSentences; s++)
                {
                    for (int w = 0; w < numWords; w++)
                    {
                        if (w > 0) { result.Append(" "); }
                        result.Append(words[rand.Next(words.Length)]);
                    }
                    result.Append(". ");
                }
                result.AppendLine();
            }

            return result.ToString();
        }
    }
}
