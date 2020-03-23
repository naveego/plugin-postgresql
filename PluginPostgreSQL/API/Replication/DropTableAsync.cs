using System.Threading.Tasks;
using PluginPostgreSQL.API.Factory;
using PluginPostgreSQL.DataContracts;

namespace PluginPostgreSQL.API.Replication
{
    public static partial class Replication
    {
        private static readonly string DropTableQuery = @"DROP TABLE IF EXISTS {0}.{1}";

        public static async Task DropTableAsync(IConnectionFactory connFactory, ReplicationTable table)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            var cmd = connFactory.GetCommand(
                string.Format(DropTableQuery,
                    Utility.Utility.GetSafeName(table.SchemaName),
                    Utility.Utility.GetSafeName(table.TableName)
                ),
                conn);
            await cmd.ExecuteNonQueryAsync();

            await conn.CloseAsync();
        }
    }
}