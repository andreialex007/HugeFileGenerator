using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace HugeFileSorting 
{
    internal static class Program
    {
        const int maxSize = 64;
        private const int hugeFileSizeMb = 2 * 1024;
        
        private static void Main(string[] args)
        {
            //1024 KB
            //1024 MB
            //1024 GB


           // Monitoring();
           // Task.Run(() => Monitoring());
          //  GenerateHugeFile("HugeFile",hugeFileSizeMb);
            var filePaths = Directory.GetFiles("Data", "*.generated-part.txt");
            MergingFiles(Path.Combine("Data","HugeFile.result.txt"), filePaths);

            // Parallel.For(0L, 10, i => GenerateHugeFile($"HugeFile-{i}.txt"));
        }
        
        
        static void MergingFiles(string outputFile, params string[] inputTxtDocs)
        {
            using (Stream outputStream = File.OpenWrite(outputFile))
            {
                foreach (string inputFile in inputTxtDocs)
                {
                    using (Stream inputStream = File.OpenRead(inputFile))
                    {
                        inputStream.CopyTo(outputStream);
                    }
                }
            }
        }

        private static void Monitoring()
        {
            DateTime startTime = DateTime.Now;
            Stopwatch stopwatch = Stopwatch.StartNew();
            while (true)
            {
                Thread.Sleep(500);
                DirectoryInfo dirInfo = new DirectoryInfo("Data");
                var sum = dirInfo.EnumerateFiles("*", SearchOption.AllDirectories).Sum(file => new System.IO.FileInfo(file.FullName).Length);
                var sizeMb = ((float) sum) / (1024 * 1024);
                
                TimeSpan timeRemaining = TimeSpan.FromTicks((long) (DateTime.Now.Subtract(startTime).Ticks * (hugeFileSizeMb - (sizeMb)) / (sizeMb)));
                
                Console.WriteLine($"Elapsed={stopwatch.Elapsed:mm':'ss};Remaining={timeRemaining:mm':'ss};SizeMB="+sizeMb);
            }
        }

        public static void MainSplittingJob()
        {
            SplitHugeFileIntoChunks();

            foreach (var filePath in Directory.GetFiles("Data", "*.part.txt"))
            {
                int[] array = File.ReadLines(filePath).Select(int.Parse).ToArray();
                Array.Sort(array);
                File.WriteAllLines(filePath.Replace(".part.txt",".sorted.part.txt"), array.Select(x=>x.ToString()));
            }
           
            var readers = GetReaders();

            var currentNumbers = readers.Select(x => int.Parse(x.ReadLine())).ToList();
            var streamWriter = new StreamWriter(Path.Combine("Data", "HugeFileSorted.txt"));
            while (true)
            {
                var minNumber = currentNumbers.Min();
                streamWriter.WriteLine(minNumber);
                
                var index = currentNumbers.IndexOf(minNumber);
                var currentReader = readers[index];
                if (currentReader.EndOfStream)
                {
                    readers.RemoveAt(index);
                    currentNumbers.RemoveAt(index);
                    if (readers.Count == 0)
                    {
                        break;
                    }
                }
                else
                {
                    currentNumbers[index] = int.Parse(currentReader.ReadLine());
                }
            }
            streamWriter.Dispose();
        }
        

        public static List<StreamReader> GetReaders()
        {
            return Directory
                .GetFiles("Data", "*.sorted.part.txt")
                .Select(x => new StreamReader(x))
                .ToList();
        }

        public static void SplitHugeFileIntoChunks()
        {
            var _100Kb = 100 * 1024;
            var hugeFilePath = Path.Combine("Data", "HugeFile.txt");

            var writerIndex = 0;
            StreamWriter currentWriter = null;
            using (var reader = new StreamReader(hugeFilePath))
            {
                while (!reader.EndOfStream)
                {
                    if (currentWriter == null || currentWriter.BaseStream.Length / _100Kb >= 1)
                    {
                        currentWriter?.Dispose();
                        writerIndex++;
                        currentWriter = new StreamWriter(Path.Combine("Data", $"FilePart-{writerIndex}.part.txt"));
                    }
                    currentWriter.WriteLine(reader.ReadLine());
                }
            }
            currentWriter?.Dispose();
        }

        private static void GenerateHugeFile(string hugeFileName, int hugeFileSizeMb)
        {
           const int hugeFilePartSizeLimitMb = 200;
           var numberOfParts = (int) Math.Round(hugeFileSizeMb / (float) hugeFilePartSizeLimitMb);
           Enumerable.Range(0,numberOfParts)
                .AsParallel()
                .ForAll(i=>GenerateHugeFilePart($"{hugeFileName}-{i}.generated-part.txt", hugeFilePartSizeLimitMb));
        }
        

        private static void GenerateHugeFilePart(string hugeFileName, int chunkSizeMb)
        {
            var mbSize = 1024 * 1024; 
            
            var hugeFilePath = Path.Combine("Data", hugeFileName);
            var random = new Random();
            var randomMaxNumber = 999_999_999;
            using (var file = new FileStream(hugeFilePath, FileMode.Create))
            {
                while (true)
                {
                    var next = random.Next(randomMaxNumber) + Environment.NewLine;
                    file.Write(Encoding.ASCII.GetBytes(next.ToString()));
                    var totalLength = file.Length;
                    if ((totalLength / mbSize) >= chunkSizeMb)
                    {
                        break;
                    }
                }
            }
            
        }
        
        
        /*
         var hugeFileText = File.ReadAllText(hugeFilePath);
            var items = hugeFileText.Split(Environment.NewLine).ToList();
            var list = items.Chunk(3).ToList();
            var sortedChunks = list.Select(x => string.Join(
                    Environment.NewLine,
                    x.Select(int.Parse).OrderBy(t => t).ToList()
                )
            ).ToList();
         * 
         */
        

        
    }

    public static class Extensions
    {
        /// <summary>
        /// Break a list of items into chunks of a specific size
        /// </summary>
        public static IEnumerable<IEnumerable<T>> Chunk<T>(this IEnumerable<T> source, int chunksize)
        {
            while (source.Any())
            {
                yield return source.Take(chunksize);
                source = source.Skip(chunksize);
            }
        }
        
        public static string ToHumanReadableString (this TimeSpan t)
        {
            if (t.TotalSeconds <= 1) {
                return $@"{t:s\.ff} seconds";
            }
            if (t.TotalMinutes <= 1) {
                return $@"{t:%s} seconds";
            }
            if (t.TotalHours <= 1) {
                return $@"{t:%m} minutes";
            }
            if (t.TotalDays <= 1) {
                return $@"{t:%h} hours";
            }

            return $@"{t:%d} days";
        }
    }
}