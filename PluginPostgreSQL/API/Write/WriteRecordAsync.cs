using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginPostgreSQL.API.Factory;
using PluginPostgreSQL.DataContracts;
using PluginPostgreSQL.Helper;

namespace PluginPostgreSQL.API.Write
{
    public static partial class Write
    {
        private static readonly SemaphoreSlim WriteSemaphoreSlim = new SemaphoreSlim(1, 1);

        public static async Task<string> WriteRecordAsync(IConnectionFactory connFactory, Schema schema, Record record,
            IServerStreamWriter<RecordAck> responseStream)
        {
            // debug
            Logger.Debug($"Starting timer for {record.RecordId}");
            var timer = Stopwatch.StartNew();

            try
            {
                var recordMap = JsonConvert.DeserializeObject<Dictionary<string, object>>(record.DataJson);

                // debug
                Logger.Debug(JsonConvert.SerializeObject(record, Formatting.Indented));

                // semaphore
                await WriteSemaphoreSlim.WaitAsync();

                // call stored procedure
                var querySb = new StringBuilder($"CALL {schema.Id}(");

                foreach (var property in schema.Properties)
                {
                    querySb.Append($"{Utility.Utility.GetSafeName(property.Id)}=>'{recordMap[property.Id]}',");
                }

                querySb.Length--;
                querySb.Append(")");

                var query = querySb.ToString();

                var conn = connFactory.GetConnection();

                await conn.OpenAsync();

                var cmd = connFactory.GetCommand(query, conn);

                await cmd.ExecuteNonQueryAsync();

                await conn.CloseAsync();

                var ack = new RecordAck
                {
                    CorrelationId = record.CorrelationId,
                    Error = ""
                };
                await responseStream.WriteAsync(ack);

                timer.Stop();
                Logger.Debug($"Acknowledged Record {record.RecordId} time: {timer.ElapsedMilliseconds}");

                return "";
            }
            catch (Exception e)
            {
                Logger.Error($"Error writing record {e.Message}");
                // send ack
                var ack = new RecordAck
                {
                    CorrelationId = record.CorrelationId,
                    Error = e.Message
                };
                await responseStream.WriteAsync(ack);

                timer.Stop();
                Logger.Debug($"Failed Record {record.RecordId} time: {timer.ElapsedMilliseconds}");

                return e.Message;
            }
            finally
            {
                WriteSemaphoreSlim.Release();
            }
        }
    }
}