using System;
using System.Threading.Tasks;
using Newtonsoft.Json;
using PluginHive.API.Factory;
using PluginHive.API.Utility;
using PluginHive.DataContracts;
using PluginHive.Helper;

namespace PluginHive.API.Replication
{
    public static partial class Replication
    {
//         private static readonly string InsertMetaDataQuery = $@"INSERT INTO {{0}}.{{1}} 
// (
// {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId)}
// , {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataRequest)}
// , {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeId)}
// , {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeName)}
// , {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataTimestamp)})
// VALUES (
// '{{2}}'
// , '{{3}}'
// , '{{4}}'
// , '{{5}}'
// , '{{6}}'
// )";
        private static readonly string InsertMetaDataQuery = $@"INSERT INTO {{0}}.{{1}} 
VALUES (
'{{2}}'
, '{{3}}'
, '{{4}}'
, '{{5}}'
, '{{6}}'
)";
        
        private static readonly string UpdateMetaDataQuery = $@"UPDATE {{0}}.{{1}}
SET 
{Utility.Utility.GetSafeName(Constants.ReplicationMetaDataRequest)} = '{{2}}'
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeId)} = '{{3}}'
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeName)} = '{{4}}'
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataTimestamp)} = '{{5}}'
WHERE {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId)} = '{{6}}'";
        
        public static async Task UpsertReplicationMetaDataAsync(IConnectionFactory connFactory, ReplicationTable table, ReplicationMetaData metaData)
        {
            return;
        //     var conn = connFactory.GetConnection();
        //     await conn.OpenAsync();
        //
        //     try
        //     {
        //         if (await RecordExistsAsync(connFactory, table, metaData.Request.DataVersions.JobId))
        //         {
        //             // update if it failed
        //             var query = string.Format(UpdateMetaDataQuery,
        //                 Utility.Utility.GetSafeName(table.SchemaName),
        //                 Utility.Utility.GetSafeName(table.TableName),
        //                 JsonConvert.SerializeObject(metaData.Request),
        //                 metaData.ReplicatedShapeId,
        //                 metaData.ReplicatedShapeName,
        //                 metaData.Timestamp,
        //                 metaData.Request.DataVersions.JobId
        //             );
        //             Logger.Debug(query);
        //             var cmd = connFactory.GetCommand(
        //                 query,
        //                 conn);
        //         
        //             await cmd.ExecuteNonQueryAsync();
        //         }
        //         else
        //         {
        //             // try to insert
        //             var query = string.Format(InsertMetaDataQuery,
        //                 Utility.Utility.GetSafeName(table.SchemaName),
        //                 Utility.Utility.GetSafeName(table.TableName),
        //                 metaData.Request.DataVersions.JobId,
        //                 JsonConvert.SerializeObject(metaData.Request),
        //                 metaData.ReplicatedShapeId,
        //                 metaData.ReplicatedShapeName,
        //                 metaData.Timestamp
        //             );
        //             Logger.Debug(query);
        //             var cmd = connFactory.GetCommand(
        //                 query,
        //                 conn);
        //             
        //             await cmd.ExecuteNonQueryAsync();
        //         }
        //     }
        //     catch (Exception e)
        //     {
        //         Logger.Error($"Error: {e.Message}");
        //         throw;
        //     }
        //
        //     await conn.CloseAsync();
        }
    }
}