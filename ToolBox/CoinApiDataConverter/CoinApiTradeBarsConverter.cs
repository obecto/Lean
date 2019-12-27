using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Consolidators;
using QuantConnect.Data.Market;
using QuantConnect.Logging;

namespace QuantConnect.ToolBox.CoinApiDataConverter
{
    public class CoinApiTradeBarsConverter
    {
        /// <summary>
        /// CoinApi Downloader Toolbox Project For LEAN Algorithmic Trading Engine.
        /// </summary>
        public static void ImportCsv(string sourceFolder = "../../../Data", string destinationFolder  = "../../../Data")
        {
            try
            {
                var symbol = Symbol.Create("ETHUSD", SecurityType.Crypto, Market.Binance);
                var dataKey = "BINANCE_SPOT_ETH_USDT";
                var csvFilePath = $"{sourceFolder}/{dataKey}.csv";
                
                Config.SetConfigurationFile($"{sourceFolder}/config.json");
                Config.Reset();
                
                var dataFolderPath = Config.Get("data-directory", destinationFolder);
                
                var consolidators = CreateConsolidators(TickType.Trade)
                    .Concat(CreateConsolidators(TickType.Quote))
                    .ToList();
                
                Console.WriteLine($"Start reading from {csvFilePath}");
                using (var reader = new StreamReader(csvFilePath))
                {
                    var line = reader.ReadLine();
                    var headers = line?.Split(';').ToList();
                    var headerMap = new Dictionary<string, int>();

                    Debug.Assert(headers != null, nameof(headers) + " != null");
                    foreach (var header in headers)
                    {
                        headerMap[header] = headers.IndexOf(header);
                    }
                    
                    while (!reader.EndOfStream)
                    {
                        line = reader.ReadLine();
                        
                        var values = line?.Split(';');

                        //time_period_start;time_period_end;time_open;time_close;price_open;price_high;price_low;price_close;volume_traded;trades_count
                        //2019-01-01T00:00:00.0000000Z;2019-01-01T00:01:00.0000000Z;2019-01-01T00:00:02.0810000Z;2019-01-01T00:00:59.6320000Z;131.450000000;131.540000000;131.420000000;131.450000000;240.203530000;87

                        var culture = CultureInfo.InvariantCulture;

                        Debug.Assert(values != null, nameof(values) + " != null");
                        var tradeBar = new TradeBar(
                            DateTime.ParseExact(values[headerMap["time_period_start"]], "yyyy-MM-ddTHH:mm:ss.ffffff0Z", culture), symbol, 
                            Convert.ToDecimal(values[headerMap["price_open"]], culture), 
                            Convert.ToDecimal(values[headerMap["price_high"]], culture),
                            Convert.ToDecimal(values[headerMap["price_low"]], culture),
                            Convert.ToDecimal(values[headerMap["price_close"]], culture),
                            Convert.ToDecimal(values[headerMap["volume_traded"]], culture),
                            TimeSpan.FromMinutes(1));
                        RegisterDataItem(tradeBar, consolidators);
                        
                        var quoteBar = new QuoteBar(tradeBar.Time, symbol, 
                            tradeBar, tradeBar.Close, tradeBar, tradeBar.Close, 
                            TimeSpan.FromMinutes(1));
                        RegisterDataItem(quoteBar, consolidators);
                    }
                }
                
                SerializeData(consolidators, symbol, dataFolderPath);
            }
            catch (Exception err)
            {
                Log.Error(err);
            }
        }
        
        private static List<BarAggregator> CreateConsolidators(TickType tickType)
        {
            var aggregators = new List<BarAggregator>();

            var resolutions = new[] {Resolution.Minute, Resolution.Hour, Resolution.Daily};
            foreach (var resolution in resolutions)
            {
                aggregators.Add(new BarAggregator(resolution, tickType));
            }

            return aggregators;
        }

        private static void RegisterDataItem(BaseData item, List<BarAggregator> consolidators)
        {
            foreach (var consolidator in consolidators)
            {
                if (item?.GetType() == consolidator.Consolidator.InputType)
                {
                    consolidator.Update(item);
                }
            }   
        }
        
        private static void SerializeData(List<BarAggregator> consolidators, Symbol symbol, 
            string dataFolderPath)
        {
            foreach (var consolidator in consolidators)
            {
                var writer = new LeanDataWriter(consolidator.Resolution, symbol, dataFolderPath, consolidator.TickType);
                writer.Write(consolidator.Flush());
            }
        }
        
        public class BarAggregator : TickAggregator
        {
            public BarAggregator(Resolution resolution, TickType tickType)
                : base(resolution, tickType)
            {
                Consolidated = new List<BaseData>();
                
                var period = Time.OneMinute;
                switch (resolution)
                {
                    case Resolution.Hour:
                        period = Time.OneHour;
                        break;
                    case Resolution.Daily:
                        period = Time.OneDay;
                        break;
                }

                if (tickType == TickType.Trade)
                {
                    Consolidator = new TradeBarConsolidator(period);
                }
                else
                {
                    Consolidator = new QuoteBarConsolidator(period);
                }
                
                Consolidator.DataConsolidated += (sender, consolidated) =>
                {
                    Consolidated.Add((BaseData) consolidated);
                };
            }
        }
    }
}