using System;
using Naveego.Sdk.Plugins;

namespace PluginPostgreSQL.DataContracts
{
    public class ReplicationMetaData
    {
        public PrepareWriteRequest Request { get; set; }
        public DateTime Timestamp { get; set; }
        public string ReplicatedShapeId { get; set; }
        public string ReplicatedShapeName { get; set; }
    }
}