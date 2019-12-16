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
                var dataDirectory = Config.Get("data-directory", "../../../Data");
                var symbolObject = Symbol.Create(tickers[0], SecurityType.Crypto, Market.GDAX);

                var tradeData = new List<BaseData>();
                var quoteData = new List<BaseData>();
                using(var reader = new StreamReader(Path.Combine(dataDirectory, "candles.csv")))
                {
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        var values = line?.Split(',').Select(x => Convert.ToDecimal(x, CultureInfo.CurrentCulture)).ToArray();

                        //0: 220.59, price_open
                        //1: 221.13, price_close
                        //2: 229.73006, price_high
                        //3: 220.17, price_low
                        //4: 358041.91919, volume_traded
                        //5: 1564942800.0
                        
                        /*
                        csv.write(f"{round(float(" +
                            "candle['price_open']),5)},{round(float(" +
                            "candle['price_close']),5)},{round(float(" +
                            "candle['price_high']),5)},{round(float(" +
                            "candle['price_low']),5)},{round(float(" +
                            "candle['volume_traded']),5)},{" +
                            "unix_t}\n")
                        */
                        
                        var tradeBar = new TradeBar(UnixTimestampToDateTime(Convert.ToDouble(values[5])), symbolObject, 
                            values[0], values[2], values[3], values[1], values[4], TimeSpan.FromMinutes(1));
                        tradeData.Add(tradeBar);
                        
                        quoteData.Add(new QuoteBar(tradeBar.Time, symbolObject, 
                            tradeBar, tradeBar.Close, tradeBar, tradeBar.Close, TimeSpan.FromMinutes(1)));
                        
                        // mono QuantConnect.ToolBox.exe --app=CoinApiDownloader --tickers=ETHUSDT --resolution=Minute --from-date=20190101-00:00:00 --to-date=20190101-00:00:00
                    }
                }
                
                Console.WriteLine(tradeData.ToString());
                
                var writer = new LeanDataWriter(Resolution.Minute, symbolObject, dataDirectory, TickType.Trade);
                writer.Write(tradeData);
                
                writer = new LeanDataWriter(Resolution.Minute, symbolObject, dataDirectory, TickType.Quote);
                writer.Write(quoteData);
                
                /*
                var downloader = new CoinApiDownloader(exchange);

                foreach (var ticker in tickers)
                {
                    // Download the data
                    var symbolObject = Symbol.Create(ticker, SecurityType.Crypto, exchange);
                    var data = downloader.Get(symbolObject, Resolution.Tick, startDate, endDate);

                    // Save the data
                    var writer = new LeanDataWriter(Resolution.Tick, symbolObject, dataDirectory, TickType.Quote);
                    writer.Write(data);
                }
                */
            }
            catch (Exception err)
            {
                Log.Error(err);
            }
        }
        
        public static DateTime UnixTimestampToDateTime(double unixTime)
        {
            DateTime unixStart = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            long unixTimeStampInTicks = (long) (unixTime * TimeSpan.TicksPerSecond);
            return new DateTime(unixStart.Ticks + unixTimeStampInTicks, System.DateTimeKind.Utc);
        }
    }
}