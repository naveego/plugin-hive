using System.Collections.Generic;
using PluginHive.DataContracts;

namespace PluginHive.API.Utility
{
    public static class Constants
    {
        public static string ReplicationRecordId = "naveego_replication_record_id";
        public static string ReplicationVersionIds = "naveego_version_ids";
        public static string ReplicationVersionRecordId = "naveego_replication_version_record_id";
        
        public static string ReplicationMetaDataTableName = "naveego_replication_metadata";
        public static string ReplicationMetaDataJobId = "naveego_job_id";
        public static string ReplicationMetaDataRequest = "request";
        public static string ReplicationMetaDataReplicatedShapeId = "naveego_shape_id";
        public static string ReplicationMetaDataReplicatedShapeName = "naveego_shape_name";
        public static string ReplicationMetaDataTimestamp = "timestamp";
        public static string ReplicationInsertTimestamp = "naveego_insert_timestamp";

        public static List<ReplicationColumn> ReplicationMetaDataColumns = new List<ReplicationColumn>
        {
            new ReplicationColumn
            {
                ColumnName = ReplicationMetaDataJobId,
                // DataType = "varchar(255)",
                DataType = "string",
                PrimaryKey = false,
                IsKey = true
            },
            new ReplicationColumn
            {
                ColumnName = ReplicationMetaDataRequest,
                PrimaryKey = false,
                // DataType = "text"
                DataType = "string",
            },
            new ReplicationColumn
            {
                ColumnName = ReplicationMetaDataReplicatedShapeId,
                // DataType = "varchar(255)",
                DataType = "string",
                PrimaryKey = false
            },
            new ReplicationColumn
            {
                ColumnName = ReplicationMetaDataReplicatedShapeName,
                // DataType = "text",
                DataType = "string",
                PrimaryKey = false
            },
            new ReplicationColumn
            {
                ColumnName = ReplicationMetaDataTimestamp,
                // DataType = "varchar(255)",
                DataType = "string",
                PrimaryKey = false
            }
        };
    }
}