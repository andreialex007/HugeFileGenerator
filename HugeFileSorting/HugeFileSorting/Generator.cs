using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace HugeFileSorting
{
    public static class Generator
    {
        private static int mbSize = 1024 * 1024;
        private static readonly int totalSizeInMb = 1 * 1024;
        private static string[] nouns;

        public static void Generate()
        {
            Directory.CreateDirectory("Data");
            nouns = File.ReadAllLines(Path.Combine("nouns.txt")).ToArray();
            Task.Run(Monitoring);
            GenerateHugeFile("HugeFile", totalSizeInMb);
            MergeHugeFileParts();
            Thread.Sleep(100);
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
            var numberRandom = new Random();
            var nounRandom = new Random();
            var hugeFilePath = Path.Combine("Data", hugeFileName);
            var randomMaxNumber = 999_999;
            
            using (var file = new FileStream(hugeFilePath, FileMode.Create))
            {
                while (true)
                {
                    var next = $"{numberRandom.Next(randomMaxNumber)}. {nouns[nounRandom.Next(0, nouns.Length)]}{Environment.NewLine}";
                    file.Write(Encoding.ASCII.GetBytes(next));
                    var totalLength = file.Length;
                    if ((totalLength / mbSize) >= chunkSizeMb)
                    {
                        break;
                    }
                }
            }
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
            var startTime = DateTime.Now;
            var stopwatch = Stopwatch.StartNew();
            while (true)
            {
                Thread.Sleep(500);
                var dirInfo = new DirectoryInfo("Data");
                var sum = dirInfo.EnumerateFiles("*", SearchOption.AllDirectories).Sum(file => new System.IO.FileInfo(file.FullName).Length);
                var currentMbSize = ((float) sum) / (mbSize);
                
                TimeSpan timeRemaining = TimeSpan.FromTicks((long) (DateTime.Now.Subtract(startTime).Ticks * (totalSizeInMb - (currentMbSize)) / (currentMbSize)));
                if(currentMbSize > totalSizeInMb)
                    return;
                Console.WriteLine($"Elapsed={stopwatch.Elapsed:mm':'ss};Remaining={timeRemaining:mm':'ss};SizeMB="+currentMbSize);
            }
        }
    }
        
        
    
}