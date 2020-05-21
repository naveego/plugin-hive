using System.Threading.Tasks;
using System.Data.Odbc;

namespace PluginHive.API.Factory
{
    public class Command : ICommand
    {
        private readonly OdbcCommand _cmd;

        public Command()
        {
            _cmd = new OdbcCommand();
        }

        public Command(string commandText)
        {
            _cmd = new OdbcCommand(commandText);
        }

        public Command(string commandText, IConnection conn)
        {
            _cmd = new OdbcCommand(commandText, (OdbcConnection) conn.GetConnection());
        }

        public void SetConnection(IConnection conn)
        {
            _cmd.Connection = (OdbcConnection) conn.GetConnection();
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