using System.Data;
using System.Data.SqlTypes;
using System.Text.Json;

using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Types;

// SQL Script to create the table
// CREATE TABLE City
// (
//     Id INT IDENTITY(1,1) PRIMARY KEY,
//     City NVARCHAR(50),
//     Location GEOMETRY,
//     Location2 GEOGRAPHY
// )

public record Location(string City, double Latitude, double Longitude);

public static class Program
{
    static readonly string connectionString = @"YourConnectionStringWhichHasCityTable";

    public static void Main()
    {
    var locations = new List<Location>
    {
        new("New York", 40.7128, -74.0060),
        new("Los Angeles", 34.0522, -118.2437),
        new("Chicago", 41.8781, -87.6298),
        new("Houston", 29.7604, -95.3698),
        new("Phoenix", 33.4484, -112.0740)
    };

        string jsonObject = JsonSerializer.Serialize(locations);

        var dt = LoadIntoDataTable(jsonObject);
        InsertIntoDatabase(dt);

        var result = QueryTable();
        foreach (DataRow row in result.Rows)
        {
            Console.WriteLine($"{row["City"]}, {row["Latitude"]}, {row["Longitude"]}");
        }
    }


    // Step 1: Load JSON object into DataTable
    static DataTable LoadIntoDataTable(string jsonObject)
    {
        var places = JsonSerializer.Deserialize<List<Location>>(jsonObject);
        var dt = new DataTable();
        dt.Columns.Add("City", typeof(string));
        dt.Columns.Add("Location", typeof(SqlGeometry));
        dt.Columns.Add("Location2", typeof(SqlGeography));

        if (places != null)
        {
            foreach (var place in places)
            {
                var wkt = new SqlChars($"POINT({place.Longitude} {place.Latitude})");
                var latlong = SqlGeometry.STPointFromText(wkt, 4326);
                var latlong2 = SqlGeography.STPointFromText(wkt, 4326);

                dt.Rows.Add(place.City, latlong, latlong2);
            }
        }

        return dt;
    }

    // Step 2: Insert DataTable into SQL Server
    static void InsertIntoDatabase(DataTable dt)
    {
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.ColumnMappings.Add("City", "City");
                bulkCopy.ColumnMappings.Add("Location", "Location");
                bulkCopy.ColumnMappings.Add("Location2", "Location2");
                bulkCopy.DestinationTableName = "City";
                bulkCopy.WriteToServer(dt);
            }
        }
    }

    // Step 3: Query the table and return the results
    static DataTable QueryTable()
    {
        var dt = new DataTable();
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();
            using var command = new SqlCommand("SELECT City, Location.STY AS Latitude, Location.STX AS Longitude FROM City", connection);
            using var reader = command.ExecuteReader();
            dt.Load(reader);
        }

        return dt;
    }
}