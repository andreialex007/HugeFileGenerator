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
        private static readonly int hugeFileSizeMb = 1 * 1024;
        private static string[] nouns;

        private static void Main(string[] args)
        {
            Directory.CreateDirectory("Data");

              GenerateHugeFile();
           // SortHugeFile();
        }

        public static void GenerateHugeFile()
        {
            nouns = File.ReadAllLines(Path.Combine("nouns.txt")).ToArray();

            Task.Run(Monitoring);
            GenerateHugeFile("HugeFile", hugeFileSizeMb);
            MergeHugeFileParts();
            Thread.Sleep(100);
            string[] strings = new List<string>().ToArray();
        }
        
        private static void MergeHugeFileParts()
        {
            var filePaths = Directory.GetFiles("Data", "*.part.txt");
            MergingFiles(Path.Combine("Data","HugeFile.result.txt"), filePaths);
        }
        
        private static void MergingFiles(string outputFile, params string[] inputTxtDocs)
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
            
            inputTxtDocs.ToList().ForEach(File.Delete);
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
                if(sizeMb > hugeFileSizeMb)
                    return;
                Console.WriteLine($"Elapsed={stopwatch.Elapsed:mm':'ss};Remaining={timeRemaining:mm':'ss};SizeMB="+sizeMb);
            }
        }
        
        public static void SortHugeFile()
        {
            SplitFile(Path.Combine("Data","HugeFile.result.txt"),1024 * 1024 * 1024, "Data");

            foreach (var filePath in Directory.GetFiles("Data", "*.part.txt"))
            {
                int[] array = File.ReadLines(filePath).Where(x=>x.Length > 0).Select(int.Parse).ToArray();
                Array.Sort(array);
                File.WriteAllLines(filePath.Replace(".part.txt",".sorted.part.txt"), array.Select(x=>x.ToString()));
            }
           
            var readers = GetReaders();
            var currentNumbers = readers.Select(x => int.Parse(x.ReadLine())).ToArray();
            var path = Path.Combine("Data", "HugeFile.sorted.txt");
            var streamWriter = new StreamWriter(path, append:false, Encoding.ASCII, 655369);
            while (true)
            {
                var minNumber = currentNumbers.Min();
                streamWriter.WriteLine(minNumber);
                
                var index = Array.IndexOf(currentNumbers, minNumber);
                var currentReader = readers[index];
                if (currentReader.EndOfStream)
                {
                    readers[index].Dispose();
                    readers.RemoveAt(index);
                    currentNumbers = currentNumbers.Where((val, idx) => idx != index).ToArray();
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

            Directory.GetFiles("Data", "*.sorted.part.txt").ToList().ForEach(File.Delete);
        }
        

        public static List<StreamReader> GetReaders()
        {
            return Directory
                .GetFiles("Data", "*.sorted.part.txt")
                .Select(x => new StreamReader(x,new FileStreamOptions{  BufferSize = 655369 }))
                .ToList();
        }
        
        public static void SplitFile(string inputFile, int chunkSize, string path)
        {
            const int BUFFER_SIZE = 20 * 1024;
            byte[] buffer = new byte[BUFFER_SIZE];

            using (Stream input = File.OpenRead(inputFile))
            {
                int index = 0;
                while (input.Position < input.Length)
                {
                    using (Stream output = File.Create($"{path}\\FilePart-{index}.part.txt"))
                    {
                        int remaining = chunkSize, bytesRead;
                        while (remaining > 0 && (bytesRead = input.Read(buffer, 0,
                            Math.Min(remaining, BUFFER_SIZE))) > 0)
                        {
                            output.Write(buffer, 0, bytesRead);
                            remaining -= bytesRead;
                        }
                    }
                    index++;
                    Thread.Sleep(500); // experimental; perhaps try it
                }
            }
        }
        

        public static void SplitHugeFileIntoChunks()
        {
            var chunckSizeMb = 100 * 1024 * 1024;
            var hugeFilePath = Path.Combine("Data", "HugeFile.result.txt");

            var writerIndex = 0;
            StreamWriter currentWriter = null;
            using (var reader = new StreamReader(hugeFilePath))
            {
                while (!reader.EndOfStream)
                {
                    if (currentWriter == null || currentWriter.BaseStream.Length / chunckSizeMb >= 1)
                    {
                        currentWriter?.Dispose();
                        writerIndex++;
                        currentWriter = new StreamWriter(Path.Combine("Data", $"FilePart-{writerIndex}.part.txt"), false, Encoding.ASCII);
                    }
                    currentWriter.WriteLine(reader.ReadLine());
                }
            }
            currentWriter?.Dispose();
        }

        private static void GenerateHugeFile(string hugeFileName, int hugeFileSizeMb)
        {
           const int hugeFilePartSizeLimitMb = 50;
           var numberOfParts = (int) Math.Round(hugeFileSizeMb / (float) hugeFilePartSizeLimitMb);
           Enumerable.Range(0,numberOfParts)
                .AsParallel()
                .ForAll(i=>GenerateHugeFilePart($"{hugeFileName}-{i}.part.txt", hugeFilePartSizeLimitMb));
        }
        

        private static void GenerateHugeFilePart(string hugeFileName, int chunkSizeMb)
        {
            var mbSize = 1024 * 1024; 
            
            var hugeFilePath = Path.Combine("Data", hugeFileName);
            var random = new Random();
            var randomMaxNumber = 999_999;
            Random nounRandom = new Random();
            using (var file = new FileStream(hugeFilePath, FileMode.Create))
            {
                while (true)
                {
                    var next = $"{random.Next(randomMaxNumber)}. {nouns[nounRandom.Next(0, nouns.Length)]}{Environment.NewLine}";
                    file.Write(Encoding.ASCII.GetBytes(next.ToString()));
                    var totalLength = file.Length;
                    if ((totalLength / mbSize) >= chunkSizeMb)
                    {
                        break;
                    }
                }
            }
            
        }
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