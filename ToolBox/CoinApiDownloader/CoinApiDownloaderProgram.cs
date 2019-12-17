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
                // mono QuantConnect.ToolBox.exe --app=CoinApiDownloader --tickers=ETHUSDT --resolution=Minute --from-date=20190101-00:00:00 --to-date=20190101-00:00:00
                //var symbolObject = Symbol.Create(tickers[0], SecurityType.Crypto, exchange);
                var symbolObject = Symbol.Create("ETHUSD", SecurityType.Crypto, "BINANCE");
                var dataKey = "BINANCE_SPOT_ETH_USDT";
                
                var apiKey = Config.Get("coinapi-api-key", "9A18C38F-6FF7-4949-8F3C-AE1E332BEE7F");
                var dataFolderPath = Config.Get("data-directory", "../../../Data");
                
                DownloadData(apiKey, dataFolderPath, symbolObject, dataKey, startDate, endDate);
            }
            catch (Exception err)
            {
                Log.Error(err);
            }
        }

        private static void DownloadData(string apiKey, string dataFolderPath, Symbol symbolObject,
            string dataKey, DateTime startDate, DateTime endDate)
        {
            var coinApi = new CoinApiRestClient(apiKey);
            var counter = startDate;
            while (counter <= endDate)
            {
                Console.WriteLine($"Downloading data for {counter}");
                
                DownloadTicks(TickType.Trade, counter, symbolObject, dataKey, dataFolderPath, coinApi);
                DownloadTicks(TickType.Quote, counter, symbolObject, dataKey, dataFolderPath, coinApi);

                counter = counter.AddDays(1);
            }
        }

        private static void DownloadTicks(TickType tickType, DateTime date, Symbol symbolObject, string dataKey,
            string dataFolderPath, CoinApiRestClient coinApi)
        {
            Console.WriteLine($"Generating {tickType} ticks");
            
            if (tickType != TickType.Trade && tickType != TickType.Quote)
            {
                throw new Exception($"Unknown tick '{tickType.ToStringInvariant()}'");
            }

            var ticks = new List<Tick>();
            var consolidators = new List<TickAggregator>();

            foreach (var resolution in new[] {Resolution.Second, Resolution.Minute, Resolution.Hour, Resolution.Daily})
            {
                if (tickType == TickType.Trade)
                {
                    consolidators.Add(new TradeTickAggregator(resolution));
                }
                else if (tickType == TickType.Quote)
                {
                    consolidators.Add(new QuoteTickAggregator(resolution));
                }
            }

            if (tickType == TickType.Trade)
            {
                var history = coinApi.Trades_historical_data(dataKey, date, date);
                foreach (var item in history)
                {
                    Tick tick = new Tick
                    {
                        Symbol = symbolObject,
                        Time = item.time_exchange,
                        Value = item.price,
                        Quantity = item.size,
                        TickType = TickType.Trade
                    };
                    ticks.Add(tick);
                    foreach (var consolidator in consolidators) 
                        consolidator.Update(tick);
                }
            } 
            else if (tickType == TickType.Quote)
            {
                var history = coinApi.Quotes_historical_data(dataKey, date, date);
                foreach (var item in history)
                {
                    Tick tick = new Tick
                    {
                        Symbol = symbolObject,
                        Time = item.time_exchange,
                        AskPrice = item.ask_price,
                        AskSize = item.ask_size,
                        BidPrice = item.bid_price,
                        BidSize = item.bid_size,
                        TickType = TickType.Quote
                    };
                    ticks.Add(tick);
                    foreach (var consolidator in consolidators) 
                        consolidator.Update(tick);
                }
            }
            
            Console.WriteLine($"Downloaded {ticks.Count} {tickType} ticks for {date}");

            var writer = new LeanDataWriter(Resolution.Tick, symbolObject, dataFolderPath, tickType);
            writer.Write(ticks);

            foreach (var consolidator in consolidators)
            {
                writer = new LeanDataWriter(consolidator.Resolution, symbolObject, dataFolderPath, tickType);
                writer.Write(consolidator.Flush());
            }
            
            Console.WriteLine($"Data for {date} is persisted");
        }
    }
}