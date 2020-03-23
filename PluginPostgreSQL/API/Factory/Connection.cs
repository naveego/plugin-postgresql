using System.Data;
using System.Threading.Tasks;
using Npgsql;
using PluginPostgreSQL.Helper;

namespace PluginPostgreSQL.API.Factory
{
    public class Connection : IConnection
    {
        private readonly NpgsqlConnection _conn;

        public Connection(Settings settings)
        {
            _conn = new NpgsqlConnection(settings.GetConnectionString());
        }

        public Connection(Settings settings, string database)
        {
            _conn = new NpgsqlConnection(settings.GetConnectionString(database));
        }

        public async Task OpenAsync()
        {
            await _conn.OpenAsync();
        }

        public async Task CloseAsync()
        {
            await _conn.CloseAsync();
        }

        public Task<bool> PingAsync()
        {
            return Task.FromResult((_conn.FullState & ConnectionState.Open) != 0);
        }

        public IDbConnection GetConnection()
        {
            return _conn;
        }
    }
}