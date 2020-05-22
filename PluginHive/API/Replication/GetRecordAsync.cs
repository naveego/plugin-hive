using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PluginHive.API.Factory;
using PluginHive.DataContracts;
using PluginHive.Helper;

namespace PluginHive.API.Replication
{
    public static partial class Replication
    {
        private static readonly string GetRecordQuery = @"SELECT * FROM {0}.{1}
WHERE {2} = '{3}'";

        public static async Task<Dictionary<string, object>> GetRecordAsync(IConnectionFactory connFactory,
            ReplicationTable table,
            string primaryKeyValue)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            var cmd = connFactory.GetCommand(string.Format(GetRecordQuery,
                    Utility.Utility.GetSafeName(table.SchemaName),
                    Utility.Utility.GetSafeName(table.TableName),
                    Utility.Utility.GetSafeName(table.Columns.Find(c => c.IsKey == true).ColumnName),
                    primaryKeyValue
                ),
                conn);
            
            var reader = await cmd.ExecuteReaderAsync();

            Dictionary<string, object> recordMap = null;
            // check if record exists
            if (reader.HasRows())
            {
                await reader.ReadAsync();

                recordMap = new Dictionary<string, object>();

                foreach (var column in table.Columns)
                {
                    try
                    {
                        recordMap[column.ColumnName] = reader.GetValueById(column.ColumnName);
                    }
                    catch (Exception e)
                    {
                        Logger.Error($"No column with column name: {column.ColumnName}");
                        Logger.Error(e.Message);
                        recordMap[column.ColumnName] = null;
                    }
                }
            }

            await conn.CloseAsync();

            return recordMap;
        }
    }
}