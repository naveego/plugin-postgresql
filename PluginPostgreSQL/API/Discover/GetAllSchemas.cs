using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginPostgreSQL.API.Factory;

namespace PluginPostgreSQL.API.Discover
{
    public static partial class Discover
    {
        private const string TableName = "TABLE_NAME";
        private const string TableSchema = "TABLE_SCHEMA";
        private const string TableType = "TABLE_TYPE";
        private const string ColumnName = "COLUMN_NAME";
        private const string DataType = "DATA_TYPE";
        private const string IsNullable = "IS_NULLABLE";
        private const string CharacterMaxLength = "CHARACTER_MAXIMUM_LENGTH";
        private const string ConstraintType = "CONSTRAINT_TYPE";

        private const string GetAllTablesAndColumnsQuery = @"
SELECT t.TABLE_NAME
     , t.TABLE_SCHEMA
     , t.TABLE_TYPE
     , c.COLUMN_NAME
	 , c.DATA_TYPE
     , c.IS_NULLABLE
     , c.CHARACTER_MAXIMUM_LENGTH
     , tc.CONSTRAINT_TYPE
FROM INFORMATION_SCHEMA.TABLES AS t
      INNER JOIN INFORMATION_SCHEMA.COLUMNS AS c ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
      LEFT OUTER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu
                      ON ccu.COLUMN_NAME = c.COLUMN_NAME AND ccu.TABLE_NAME = t.TABLE_NAME AND
                         ccu.TABLE_SCHEMA = t.TABLE_SCHEMA
      LEFT OUTER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                      ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME AND tc.CONSTRAINT_SCHEMA = ccu.CONSTRAINT_SCHEMA
WHERE t.TABLE_SCHEMA NOT IN ('information_schema', 'pg_catalog')
AND (tc.CONSTRAINT_TYPE IS NULL OR tc.CONSTRAINT_TYPE = 'PRIMARY KEY')
ORDER BY t.TABLE_NAME, t.TABLE_SCHEMA";

        public static async IAsyncEnumerable<Schema> GetAllSchemas(IConnectionFactory connFactory, int sampleSize = 5)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            var cmd = connFactory.GetCommand(GetAllTablesAndColumnsQuery, conn);
            var reader = await cmd.ExecuteReaderAsync();

            Schema schema = null;
            var currentSchemaId = "";
            while (await reader.ReadAsync())
            {
                var schemaId =
                    $"{Utility.Utility.GetSafeName(reader.GetValueById(TableSchema).ToString())}.{Utility.Utility.GetSafeName(reader.GetValueById(TableName).ToString())}";
                if (schemaId != currentSchemaId)
                {
                    // return previous schema
                    if (schema != null)
                    {
                        // get sample and count
                        yield return await AddSampleAndCount(connFactory, schema, sampleSize);
                    }

                    // start new schema
                    currentSchemaId = schemaId;
                    var parts = DecomposeSafeName(currentSchemaId).TrimEscape();
                    schema = new Schema
                    {
                        Id = currentSchemaId,
                        Name = $"{parts.Schema}.{parts.Table}",
                        Properties = { },
                        DataFlowDirection = Schema.Types.DataFlowDirection.Read
                    };
                }

                // add column to schema
                var property = new Property
                {
                    Id = Utility.Utility.GetSafeName(reader.GetValueById(ColumnName).ToString()),
                    Name = reader.GetValueById(ColumnName).ToString(),
                    IsKey = reader.GetValueById(ConstraintType).ToString() == "PRIMARY KEY",
                    IsNullable = reader.GetValueById(IsNullable).ToString() == "YES",
                    Type = GetType(reader.GetValueById(DataType).ToString()),
                    TypeAtSource = GetTypeAtSource(reader.GetValueById(DataType).ToString(),
                        reader.GetValueById(CharacterMaxLength))
                };
                schema?.Properties.Add(property);
            }

            await conn.CloseAsync();

            if (schema != null)
            {
                // get sample and count
                yield return await AddSampleAndCount(connFactory, schema, sampleSize);
            }
        }

        private static async Task<Schema> AddSampleAndCount(IConnectionFactory connFactory, Schema schema,
            int sampleSize)
        {
            // add sample and count
            var records = Read.Read.ReadRecords(connFactory, schema).Take(sampleSize);
            schema.Sample.AddRange(await records.ToListAsync());
            schema.Count = await GetCountOfRecords(connFactory, schema);

            return schema;
        }

        public static PropertyType GetType(string dataType)
        {
            switch (dataType)
            {
                case var t when t.Contains("timestamp"):
                    return PropertyType.Datetime;
                case "date":
                    return PropertyType.Date;
                case "time":
                    return PropertyType.Time;
                case "smallint":
                case "int":
                case "integer":
                case "bigint":
                    return PropertyType.Integer;
                case "decimal":
                    return PropertyType.Decimal;
                case "real":
                case "float":
                case "double":
                    return PropertyType.Float;
                case "boolean":
                case "bit":
                    return PropertyType.Bool;
                case "blob":
                case "mediumblob":
                case "longblob":
                    return PropertyType.Blob;
                case "char":
                case "character":
                case "varchar":
                case "tinytext":
                    return PropertyType.String;
                case "text":
                case "mediumtext":
                case "longtext":
                    return PropertyType.Text;
                default:
                    return PropertyType.String;
            }
        }

        private static string GetTypeAtSource(string dataType, object maxLength)
        {
            return maxLength != null ? $"{dataType}({maxLength})" : dataType;
        }
    }
}