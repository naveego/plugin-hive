using Naveego.Sdk.Plugins;
using PluginHive.API.Utility;
using PluginHive.DataContracts;

namespace PluginHive.API.Replication
{
    public static partial class Replication
    {
        public static ReplicationTable GetVersionReplicationTable(Schema schema, string safeSchemaName, string safeVersionTableName)
        {
            var versionTable = ConvertSchemaToReplicationTable(schema, safeSchemaName, safeVersionTableName);
            versionTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationVersionRecordId,
                // DataType = "varchar(255)",
                DataType = "string",
                PrimaryKey = false,
                IsKey = true
            });
            versionTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationRecordId,
                // DataType = "varchar(255)",
                DataType = "string",
                PrimaryKey = false
            });

            return versionTable;
        }
    }
}