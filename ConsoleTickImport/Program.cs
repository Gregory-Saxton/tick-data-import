using System;
using Npgsql;
using PostgreSQLCopyHelper;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Security.Cryptography;

namespace ConsoleTickImport
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Postgresql Bulk Copy Mode engaged...");

            var bulkCopyHelper = new PostgreSQLCopyHelper<TickDataTableEntry>("ticks", "StandardAndPoorsMinuteCandlestick")
            .MapInteger("epoch", x=>x.Epoch)
            .MapCharacter("symbol", x=>x.Symbol)
            .MapMoney("open", x=>x.Open)
            .MapMoney("close", x=>x.Close)
            .MapMoney("high", x=>x.High)
            .MapMoney("low", x=>x.Low)
            .MapInteger("volume", x=>x.Volume)
            .MapBigInt("composite_key", x=>x.CompositeKey);

            string [] fileEntries = Directory.GetFiles(@"C:\Users\sdgre\Documents\sp500_kaggle_cleaned", "*.csv",  SearchOption.TopDirectoryOnly);
            MD5 md5Hasher = MD5.Create();
            string symbolName ="";            
            var integerHash ="";

            foreach(string fileName in fileEntries){
                List<TickDataTableEntry> tickData = new List<TickDataTableEntry>();
                symbolName = Path.GetFileNameWithoutExtension(fileName);
                
                var hashed  = md5Hasher.ComputeHash(Encoding.UTF8.GetBytes(symbolName));
                integerHash = Math.Abs(BitConverter.ToInt32(hashed, 0)).ToString();                                       
                integerHash = integerHash.Substring(0, integerHash.Length-1);
                
                using(var csvReader = new StreamReader(fileName)){
                    while(!csvReader.EndOfStream){
                        var line = csvReader.ReadLine();
                        var values = line.Split(',');

                        if(values[0] != "timestamp"){                            
                            TickDataTableEntry Tick = new TickDataTableEntry();                            
                            Tick.CompositeKey = Int64.Parse(integerHash + values[0]);
                            Tick.Epoch = Int32.Parse(values[0]);
                            Tick.Open = Decimal.Parse(values[1]);
                            Tick.Close = Decimal.Parse(values[2]);
                            Tick.High = Decimal.Parse(values[3]);
                            Tick.Low = Decimal.Parse(values[4]);
                            Tick.Volume = Int32.Parse(values[5]);
                            Tick.Symbol = symbolName;
                            tickData.Add(Tick);                            
                        }                                                                                                
                    }
                }

                WriteBulkCopyLoad(bulkCopyHelper, tickData);
            }                        
        }

        private static void WriteBulkCopyLoad(PostgreSQLCopyHelper<TickDataTableEntry> copyHelper, List<TickDataTableEntry> ticks){            
            using (var connection = new NpgsqlConnection(""))
            {
                connection.Open();

                copyHelper.SaveAll(connection, ticks);
            }
        }
    }

    public class TickDataTableEntry{
        public Int32 Epoch{ get; set;}
        public string Symbol{ get; set;}
        public Decimal Open{ get; set;}
        public Decimal Close{ get; set;}
        public Decimal High{ get; set;}
        public Decimal Low{ get; set;}
        public Int32 Volume{ get; set;}
        public Int64 CompositeKey{ get; set;}        
    }
}
