using System.IO;

namespace HugeFileSorting 
{
    internal static class Program
    {
        private static void Main(string[] args)
        {
            Directory.CreateDirectory("Data");
            Generator.Generate();
            Sorter.Sort();
        }
    }
}