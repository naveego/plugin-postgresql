using System.Threading.Tasks;
using Npgsql;

namespace PluginPostgreSQL.API.Factory
{
    public class Command : ICommand
    {
        private readonly NpgsqlCommand _cmd;

        public Command()
        {
            _cmd = new NpgsqlCommand();
        }

        public Command(string commandText)
        {
            _cmd = new NpgsqlCommand(commandText);
        }

        public Command(string commandText, IConnection conn)
        {
            _cmd = new NpgsqlCommand(commandText, (NpgsqlConnection) conn.GetConnection());
        }

        public void SetConnection(IConnection conn)
        {
            _cmd.Connection = (NpgsqlConnection) conn.GetConnection();
        }

        public void SetCommandText(string commandText)
        {
            _cmd.CommandText = commandText;
        }

        public void AddParameter(string name, object value)
        {
            _cmd.Parameters.AddWithValue(name, value);
        }

        public async Task<IReader> ExecuteReaderAsync()
        {
            return new Reader(await _cmd.ExecuteReaderAsync());
        }

        public async Task<int> ExecuteNonQueryAsync()
        {
            return await _cmd.ExecuteNonQueryAsync();
        }
    }
}