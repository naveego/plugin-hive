using System.Data;
using System.Data.Odbc;
using System.Threading.Tasks;
using PluginHive.Helper;

namespace PluginHive.API.Factory
{
    public class Connection : IConnection
    {
        private readonly OdbcConnection _conn;

        public Connection(Settings settings)
        {
            _conn = new OdbcConnection(settings.GetConnectionString());
        }

        public Connection(Settings settings, string database)
        {
            _conn = new OdbcConnection(settings.GetConnectionString(database));
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
            return Task.FromResult((_conn.State & ConnectionState.Open) != 0);
        }

        public IDbConnection GetConnection()
        {
            return _conn;
        }
    }
}