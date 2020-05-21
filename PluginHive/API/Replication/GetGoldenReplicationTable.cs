using Naveego.Sdk.Plugins;
using PluginHive.API.Utility;
using PluginHive.DataContracts;

namespace PluginHive.API.Replication
{
    public static partial class Replication
    {
        public static ReplicationTable GetGoldenReplicationTable(Schema schema, string safeSchemaName, string safeGoldenTableName)
        {
            var goldenTable = ConvertSchemaToReplicationTable(schema, safeSchemaName, safeGoldenTableName);
            goldenTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationRecordId,
                // DataType = "varchar(255)",
                DataType = "string",
                PrimaryKey = true
            });
            goldenTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationVersionIds,
                // DataType = "text",
                DataType = "string",
                PrimaryKey = false,
                Serialize = true
            });

            return goldenTable;
        }
    }
}