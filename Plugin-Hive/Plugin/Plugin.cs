using System;
using System.Threading.Tasks;
using Grpc.Core;
using System.Data.CData.ApacheHive;
using Newtonsoft.Json;
using Plugin_Hive.DataContracts;
using Plugin_Hive.Helper;
using Pub;


namespace Plugin_Hive.Plugin
{
    public class Plugin : Publisher.PublisherBase
    {
     
        private FormSettings _formSettings;
  
        private TaskCompletionSource<bool> _tcs;

        private volatile bool _connected;

        private ApacheHiveConnection _conn = null;
      
        
        public Plugin()
        {
        }

        /// <summary>
        /// Establishes a connection with Naveego Legacy CRM. Creates an authenticated http client and tests it.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns>A message indicating connection success</returns>
        public override async Task<ConnectResponse> Connect(ConnectRequest request, ServerCallContext context)
        {
            try
            {
                _formSettings = JsonConvert.DeserializeObject<FormSettings>(request.SettingsJson) ?? new FormSettings();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return new ConnectResponse
                {
                    ConnectionError = "",
                    OauthError = "",
                    SettingsError = e.Message
                };
            }

            try
            {
                var conn = GetOrCreateConnection();
                _connected = true;

                return new ConnectResponse
                {
                    ConnectionError = "",
                    OauthError = "",
                    SettingsError = ""
                };
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return new ConnectResponse
                {
                    ConnectionError = e.Message,
                    OauthError = "",
                    SettingsError = ""
                };
            }
        }

        public override async Task ConnectSession(ConnectRequest request,
            IServerStreamWriter<ConnectResponse> responseStream, ServerCallContext context)
        {
            Logger.Info("Connecting session...");

            // create task to wait for disconnect to be called
            _tcs?.SetResult(true);
            _tcs = new TaskCompletionSource<bool>();

            // call connect method
            var response = await Connect(request, context);

            await responseStream.WriteAsync(response);

            Logger.Info("Session connected.");

            // wait for disconnect to be called
            await _tcs.Task;
        }

        /// <summary>
        /// Handles disconnect requests from the agent
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<DisconnectResponse> Disconnect(DisconnectRequest request, ServerCallContext context)
        {
            // alert connection session to close
            if (_tcs != null)
            {
                _tcs.SetResult(true);
                _tcs = null;
            }

            if (_conn != null)
            {
                _conn.Close();
                _conn = null;
            }
            
            _connected = false;

            Logger.Info("Disconnected");
            return Task.FromResult(new DisconnectResponse());
        }


        /// <summary>
        /// Discovers schemas located in the users Zoho CRM instance
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns>Discovered schemas</returns>
        public override async Task<DiscoverSchemasResponse> DiscoverSchemas(DiscoverSchemasRequest request,
            ServerCallContext context)
        {
            Logger.Info("Discovering Schemas...");

            DiscoverSchemasResponse discoverSchemasResponse = new DiscoverSchemasResponse();

            try
            {
                var conn = GetOrCreateConnection();
                conn.
                
                return discoverSchemasResponse;
            }
            catch (Exception ex)
            {
                Logger.Error("Could not discover schemas: " + ex.ToString());
                throw;
            }  
        }

        /// <summary>
        /// Publishes a stream of data for a given schema
        /// </summary>
        /// <param name="request"></param>
        /// <param name="responseStream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        /*
        public override async Task ReadStream(ReadRequest request, IServerStreamWriter<Record> responseStream,
            ServerCallContext context)
        {
            var schema = request.Schema;
            var dynamicObject = DynamicObject.GetByName(schema.Id);
            var limit = request.Limit;
            var limitFlag = request.Limit != 0;

            Logger.Info($"Publishing records for schema: {schema.Name}");

            try
            {
                var recordsCount = 0;
                var records = new List<Dictionary<string, object>>();

                // get all records
                // build query string
                StringBuilder query = new StringBuilder("select+");

                foreach (var property in schema.Properties)
                {
                    query.Append($"{property.Id},");
                }

                // remove trailing comma
                query.Length--;

                query.Append($"+from+{schema.Id}");

                ApiRecords apiRecords;
                long offset = 0;

                do
                {
                    // get records for schema page by page
                    apiRecords = await _hubSpotClient.GetRecords(dynamicObject, offset);
                    records.AddRange((apiRecords.Records));
                    offset = apiRecords.Offset;
                } while (apiRecords.HasMore && _connected);

                // Publish records for the given schema
                foreach (var record in records)
                {
                    var recordOutput = new Record
                    {
                        Action = Record.Types.Action.Upsert,
                        DataJson = JsonConvert.SerializeObject(record)
                    };

                    // stop publishing if the limit flag is enabled and the limit has been reached
                    if ((limitFlag && recordsCount == limit) || !_connected)
                    {
                        break;
                    }

                    // publish record
                    await responseStream.WriteAsync(recordOutput);
                    recordsCount++;
                }

                Logger.Info($"Published {recordsCount} records");
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }
        
        */

        private ApacheHiveConnection GetOrCreateConnection()
        {
            if (_conn != null)
            {
                return _conn;
            }
            
            var connectString = $"Server={_formSettings.ServerName};Port={_formSettings.Port};TransportMode=BINARY";
            _conn = new ApacheHiveConnection(connectString);
            _conn.Open();
            
           
            return _conn;
        }
       
        
    }
}