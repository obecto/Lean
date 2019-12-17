/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using CoinAPI.REST.V1;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.ToolBox.CoinApiDownloader
{
    public static class CoinApiDownloaderProgram
    {
        /// <summary>
        /// CoinApi Downloader Toolbox Project For LEAN Algorithmic Trading Engine.
        /// </summary>
        public static void CoinApiDownloader(IList<string> tickers, string exchange, DateTime startDate, DateTime endDate)
        {
            if (exchange.IsNullOrEmpty() || tickers.IsNullOrEmpty())
            {
                Console.WriteLine("CoinApiDownloader ERROR: '--exchange' or '--tickers' parameter is missing");
                Environment.Exit(1);
            }

            try
            {
                Config.SetConfigurationFile("../../../Data/config.json");
                Config.Reset();
                
                // mono QuantConnect.ToolBox.exe --app=CoinApiDownloader --tickers=ETHUSDT --resolution=Minute --from-date=20190101-00:00:00 --to-date=20190101-00:00:00
                //var symbolObject = Symbol.Create(tickers[0], SecurityType.Crypto, exchange);
                var symbol = Symbol.Create("ETHUSD", SecurityType.Crypto, "BINANCE");
                var dataKey = "BINANCE_SPOT_ETH_USDT";
                
                var apiKey = Config.Get("coinapi-api-key");
                var dataFolderPath = Config.Get("data-directory", "../../../Data");
                
                var coinApi = new CoinApiRestClient(apiKey);
            
                var counter = startDate;
                while (counter <= endDate)
                {
                    Console.WriteLine($"--- Downloading data for {counter}");
                
                    DownloadData(counter, symbol, TickType.Trade, coinApi, dataKey, dataFolderPath);
                    DownloadData(counter, symbol, TickType.Quote, coinApi, dataKey, dataFolderPath);
                
                    Console.WriteLine($"Finished processing data for {counter}");
                    
                    counter = counter.AddDays(1);
                }
            }
            catch (Exception err)
            {
                Log.Error(err);
            }
        }

        private static void DownloadData(DateTime date, Symbol symbol, TickType tickType,
            CoinApiRestClient coinApi, string dataKey, string dataFolderPath)
        {
            var downloadBatchSize = 100;
            var persistBatchSize = 10 * downloadBatchSize;
            
            var aggregators = ConstructAggregators(tickType);

            var counter = 0;
            var endDate = date.AddDays(1);
            var cursorDate = date;
            while (cursorDate < endDate)
            {
                IEnumerable history;
            
                if (tickType == TickType.Trade)
                {
                    history = coinApi.Trades_historical_data(dataKey, cursorDate, downloadBatchSize);
                }
                else if (tickType == TickType.Quote)
                {
                    history = coinApi.Quotes_historical_data(dataKey, cursorDate, downloadBatchSize);
                }
                else
                {
                    throw new Exception($"Unknown tick type '{tickType}'");
                }

                
                foreach (var item in history)
                {
                    counter++;
                    
                    if (item is Trade)
                    {
                        cursorDate = ((Trade)item).time_coinapi;
                    }
                    else if (item is Quote)
                    {
                        cursorDate = ((Quote)item).time_coinapi;
                    }
                    else
                    {
                        throw new Exception($"Unknown data item '{item}'");
                    }

                    RegisterDataItem(item, dataKey, aggregators);
                }


                if (cursorDate >= endDate || counter % persistBatchSize == 0)
                {
                    SerializeData(aggregators, symbol, dataFolderPath);
                    Console.WriteLine($"Snapshot at {tickType} tick {counter} for {date}");
                }
            }
            
            Console.WriteLine($"Persisted {counter} {tickType} ticks for {date}");
        }
        
        private static List<TickAggregator> ConstructAggregators(TickType tickType)
        {
            var aggregators = new List<TickAggregator>();

            aggregators.Add(new IdentityTickAggregator(tickType));

            var resolutions = new[] {Resolution.Minute, Resolution.Hour, Resolution.Daily};
            foreach (var resolution in resolutions)
            {
                if (tickType == TickType.Trade)
                    
                {
                    aggregators.Add(new TradeTickAggregator(resolution));
                }
                else if (tickType == TickType.Quote)
                {
                    aggregators.Add(new QuoteTickAggregator(resolution));
                }
            }

            return aggregators;
        }

        private static void RegisterDataItem(object item, Symbol symbol, List<TickAggregator> aggregators)
        {
            Tick tick;

            if (item is Trade)
            {
                var tradeItem = (Trade)item;
                tick = new Tick
                {
                    Symbol = symbol,
                    Time = tradeItem.time_exchange,
                    Value = tradeItem.price,
                    Quantity = tradeItem.size,
                    TickType = TickType.Trade
                };
            }
            else if (item is Quote)
            {
                var quoteItem = (Quote) item;
                tick = new Tick
                {
                    Symbol = symbol,
                    Time = quoteItem.time_exchange,
                    AskPrice = quoteItem.ask_price,
                    AskSize = quoteItem.ask_size,
                    BidPrice = quoteItem.bid_price,
                    BidSize = quoteItem.bid_size,
                    TickType = TickType.Quote
                };
            }
            else
            {
                throw new Exception($"Unknown data item '{item}'");
            }

            foreach (var consolidator in aggregators)
            {
                consolidator.Update(tick);
            }   
        }
        
        private static void SerializeData(List<TickAggregator> aggregators, Symbol symbol, 
            string dataFolderPath)
        {
            foreach (var consolidator in aggregators)
            {
                var writer = new LeanDataWriter(consolidator.Resolution, symbol, dataFolderPath, consolidator.TickType);
                writer.Write(consolidator.Flush());
            }
        }
    }
}