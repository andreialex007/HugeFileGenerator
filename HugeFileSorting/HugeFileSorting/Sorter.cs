using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace HugeFileSorting
{
    public static class Sorter
    {
        public static void Sort()
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
        
        private static List<StreamReader> GetReaders()
        {
            return Directory
                .GetFiles("Data", "*.sorted.part.txt")
                .Select(x => new StreamReader(x,new FileStreamOptions{  BufferSize = 655369 }))
                .ToList();
        }
        
        private static void SplitFile(string inputFile, int chunkSize, string path)
        {
            const int bufferSize = 20 * 1024;
            byte[] buffer = new byte[bufferSize];

            using (Stream input = File.OpenRead(inputFile))
            {
                int index = 0;
                while (input.Position < input.Length)
                {
                    using (Stream output = File.Create($"{path}\\FilePart-{index}.part.txt"))
                    {
                        int remaining = chunkSize, bytesRead;
                        while (remaining > 0 && (bytesRead = input.Read(buffer, 0,
                            Math.Min(remaining, bufferSize))) > 0)
                        {
                            output.Write(buffer, 0, bytesRead);
                            remaining -= bytesRead;
                        }
                    }
                    index++;
                    Thread.Sleep(10);
                }
            }
        }
        
    }
}