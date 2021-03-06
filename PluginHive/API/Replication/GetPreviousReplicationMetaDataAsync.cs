using System;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginHive.API.Factory;
using PluginHive.DataContracts;
using PluginHive.Helper;
using Constants = PluginHive.API.Utility.Constants;

namespace PluginHive.API.Replication
{
    public static partial class Replication
    {
        private static readonly string GetMetaDataQuery = @"SELECT * FROM {0}.{1} WHERE {2} = '{3}'";

        public static async Task<ReplicationMetaData> GetPreviousReplicationMetaDataAsync(IConnectionFactory connFactory,
            string jobId,
            ReplicationTable table)
        {
            return null;
            // try
            // {
            //     ReplicationMetaData replicationMetaData = null;
            //
            //     // ensure replication metadata table
            //     await EnsureTableAsync(connFactory, table);
            //
            //     // check if metadata exists
            //     var conn = connFactory.GetConnection();
            //     await conn.OpenAsync();
            //
            //     var cmd = connFactory.GetCommand(
            //         string.Format(GetMetaDataQuery, 
            //             Utility.Utility.GetSafeName(table.SchemaName),
            //             Utility.Utility.GetSafeName(table.TableName), 
            //             Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId),
            //             jobId),
            //         conn);
            //     var reader = await cmd.ExecuteReaderAsync();
            //
            //     if (reader.HasRows())
            //     {
            //         // metadata exists
            //         await reader.ReadAsync();
            //
            //         Logger.Debug(reader.GetValueById($"{table.TableName}.{Constants.ReplicationMetaDataRequest}").ToString());
            //         
            //         var request = JsonConvert.DeserializeObject<PrepareWriteRequest>(
            //             reader.GetValueById($"{table.TableName}.{Constants.ReplicationMetaDataRequest}").ToString());
            //         var shapeName = reader.GetValueById($"{table.TableName}.{Constants.ReplicationMetaDataReplicatedShapeName}")
            //             .ToString();
            //         var shapeId = reader.GetValueById($"{table.TableName}.{Constants.ReplicationMetaDataReplicatedShapeId}")
            //             .ToString();
            //         var timestamp = DateTime.Parse(reader.GetValueById($"{table.TableName}.{Constants.ReplicationMetaDataTimestamp}")
            //             .ToString());
            //         
            //         replicationMetaData = new ReplicationMetaData
            //         {
            //             Request = request,
            //             ReplicatedShapeName = shapeName,
            //             ReplicatedShapeId = shapeId,
            //             Timestamp = timestamp
            //         };
            //     }
            //
            //     await conn.CloseAsync();
            //
            //     return replicationMetaData;
            // }
            // catch (Exception e)
            // {
            //     Logger.Error(e, e.Message);
            //     Logger.Error(e.StackTrace);
            //     Logger.Error(e.Source);
            //     throw;
            // }
        }
    }
}