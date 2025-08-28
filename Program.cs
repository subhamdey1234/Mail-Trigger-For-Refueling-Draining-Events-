using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Mail;
using System.Text.Json;
using Npgsql;
using System.Text;
using System.Threading.Tasks;
using System.Linq;

class ConnectionStrings
{
    public string? CitusDb { get; set; }
}

class TestData
{
    public List<string>? AssetCodes { get; set; }
    public string? StartTime { get; set; }
    public string? EndTime { get; set; }
}

class EmailSettings
{
    public string? Sender { get; set; }
    public string? AppPassword { get; set; }
    public List<string>? Recipients { get; set; }
    public List<string>? CCEmails { get; set; }
}

class AppSettings
{
    public ConnectionStrings? ConnectionStrings { get; set; }
    public TestData? TestData { get; set; }
    public EmailSettings? Email { get; set; }
}

class FuelRecord
{
    public object? DeviceId;
    public double FuelLevel;
    public DateTime Rtc;
    public double Latitude;
    public double Longitude;
    public double Speed;
}

class Program
{
    static async Task Main(string[] args)
    {
        try
        {
            var appSettings = LoadAppSettings();
            if (appSettings == null || string.IsNullOrEmpty(appSettings.ConnectionStrings?.CitusDb))
            {
                Console.WriteLine("Database connection string is missing.");
                return;
            }

            var testData = appSettings.TestData;
            if (testData?.AssetCodes == null || testData.AssetCodes.Count == 0)
            {
                Console.WriteLine("No asset codes provided for testing.");
                return;
            }

            if (!DateTime.TryParseExact(testData.StartTime, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime startTime) ||
                !DateTime.TryParseExact(testData.EndTime, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime endTime))
            {
                Console.WriteLine("Invalid start or end time format.");
                return;
            }

            // Refactor to ensure single table header
            StringBuilder allReports = new();
            allReports.AppendLine("<table style='border: 1px solid #ddd; border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;'>");
            allReports.AppendLine("<thead style='background-color: #f4f4f4;'><tr>");
            allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>ASSET_CODE</th>");
            allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>JOBCODE</th>");
            allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>START_TIME</th>");
            allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>END_TIME</th>");
            allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>START_VALUE</th>");
            allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>END_VALUE</th>");
            allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>AMOUNT</th>");
            allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>TYPE</th>");
            allReports.AppendLine("</tr></thead>");
            allReports.AppendLine("<tbody>");

            List<string> refueledAssets = new List<string>();

            var assetCodesFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..", "..", "..", "d_lntassetmaster_202508141155.json");
            var assetCodes = LoadAssetCodesFromJson(assetCodesFilePath);
            if (assetCodes.Count == 0)
            {
                Console.WriteLine("No asset codes found in the JSON file.");
                return;
            }

            foreach (var assetCode in assetCodes)
            {
                Console.WriteLine($"\n--- Processing AssetCode: {assetCode} ---");

                if (!HasSensorsInstalled(appSettings.ConnectionStrings.CitusDb, assetCode))
                {
                    Console.WriteLine($"Skipping asset code {assetCode} as sensors are not installed.");
                    continue;
                }

                if (!TryGetDeviceId(appSettings.ConnectionStrings.CitusDb, assetCode, out int deviceId, out string? jobCode))
                {
                    Console.WriteLine($"No device found for asset code {assetCode}.");
                    continue;
                }

                jobCode ??= "Unknown Job Code"; // Fallback value for jobCode

                var records = FetchFuelRecords(appSettings.ConnectionStrings.CitusDb, deviceId, startTime, endTime);
                Console.WriteLine($"Fetched {records.Count} records for asset code {assetCode}.");
                if (records.Count == 0)
                {
                    Console.WriteLine($"No records found for asset code {assetCode}.");
                    continue;
                }

                var (report, totalRefueled) = AnalyzeFuelData(records, jobCode, assetCode);
                if (totalRefueled > 0) // Check if total refueled value is greater than zero
                {
                    allReports.AppendLine(report);
                    refueledAssets.Add(assetCode); // Add to refueled assets list
                }
                else
                {
                    Console.WriteLine($"No refuel events detected for asset code {assetCode}.");
                }
            }

            allReports.AppendLine("</tbody></table>");

            if (refueledAssets.Count > 0) // Send email only if there are refueled assets
            {
                Console.WriteLine("\n--- Sending Email with All Reports ---\n");
                if (appSettings.Email == null || string.IsNullOrEmpty(appSettings.Email.Sender) || string.IsNullOrEmpty(appSettings.Email.AppPassword) || appSettings.Email.Recipients == null || appSettings.Email.Recipients.Count == 0)
                {
                    Console.WriteLine("Email settings are missing or incomplete.");
                }
                else
                {
                    // Update Main method to use recipients and CC emails from appsettings.json
                    await SendEmailReportAsync("Fuel Report", allReports.ToString(), appSettings.Email.Recipients, appSettings.Email.Recipients);
                }
            }
            else
            {
                Console.WriteLine("No refueled assets found. No email will be sent.");
            }

            // Add functionality to search for a specific asset code and display refuel data
            if (args.Length > 0 && args[0].Equals("search", StringComparison.OrdinalIgnoreCase))
            {
                if (args.Length < 2)
                {
                    Console.WriteLine("Please provide an asset code to search.");
                    return;
                }

                string assetCodeToSearch = args[1];
                SearchAssetCodeAndDisplayRefuelData(appSettings.ConnectionStrings.CitusDb, assetCodeToSearch, startTime, endTime);
                return;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }

    static AppSettings? LoadAppSettings()
    {
        string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "appsettings.json");
        if (!File.Exists(path))
        {
            Console.WriteLine("appsettings.json not found.");
            return null;
        }
        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize<AppSettings>(json);
    }

    // Fixing nullable issue in TryGetDeviceId method
    static bool TryGetDeviceId(string connString, string assetCode, out int deviceId, out string? jobCode)
    {
        deviceId = 0;
        jobCode = null;
        using var conn = new NpgsqlConnection(connString);
        conn.Open();
        using var cmd = new NpgsqlCommand("SELECT platform_asset_id, jobcode FROM \"Fuel_Prod\".d_lntassetmaster WHERE asset_code = @assetCode", conn);
        cmd.Parameters.AddWithValue("assetCode", assetCode);
        using var reader = cmd.ExecuteReader();
        if (reader.Read())
        {
            deviceId = reader.GetInt32(0);
            jobCode = reader.IsDBNull(1) ? "Unknown Job Code" : reader.GetString(1); // Ensure jobCode is never null
            return true;
        }
        return false;
    }

    static List<FuelRecord> FetchFuelRecords(string connString, int deviceId, DateTime startTime, DateTime endTime)
    {
        var list = new List<FuelRecord>();
        using var conn = new NpgsqlConnection(connString);
        conn.Open();
        using var cmd = new NpgsqlCommand("SELECT deviceid, fuel_level, rtc, tag_386, tag_387, tag_388 FROM \"Fuel_Prod\".m_fuel_metric_calc WHERE deviceid = @deviceId AND rtc BETWEEN @start AND @end ORDER BY rtc ASC", conn);
        cmd.Parameters.AddWithValue("deviceId", deviceId);
        cmd.Parameters.AddWithValue("start", startTime);
        cmd.Parameters.AddWithValue("end", endTime);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            list.Add(new FuelRecord
            {
                DeviceId = reader.IsDBNull(reader.GetOrdinal("deviceid")) ? null : reader["deviceid"],
                FuelLevel = reader.IsDBNull(reader.GetOrdinal("fuel_level")) ? 0 : Convert.ToDouble(reader["fuel_level"]),
                Rtc = reader.IsDBNull(reader.GetOrdinal("rtc")) ? DateTime.MinValue : Convert.ToDateTime(reader["rtc"]),
                Latitude = reader.IsDBNull(reader.GetOrdinal("tag_386")) ? 0 : Convert.ToDouble(reader["tag_386"]),
                Longitude = reader.IsDBNull(reader.GetOrdinal("tag_387")) ? 0 : Convert.ToDouble(reader["tag_387"]),
                Speed = reader.IsDBNull(reader.GetOrdinal("tag_388")) ? 0 : Convert.ToDouble(reader["tag_388"])
            });
        }
        return list;
    }

    // Ensure only one table header in AnalyzeFuelData method
    static (string report, double totalRefueled) AnalyzeFuelData(List<FuelRecord> records, string jobCode, string assetCode)
    {
        StringBuilder report = new();
        double refuelTotal = 0;
        const double minRefuelThreshold = 25.0;
        const double maxRefuelThreshold = 700.0;
        const double stationarySpeedThreshold = 0.5; // speed < 0.5 means stationary
        const double undulationThreshold = 5.0; // Ignore changes smaller than this value
        const int stabilityWindow = 3; // Number of records to check for stability

        if (records == null || records.Count == 0)
        {
            report.AppendLine("<p style='font-family: Arial, sans-serif; color: #333;'>No fuel records to analyze.</p>");
            return (report.ToString(), refuelTotal);
        }

        int i = 0;
        while (i < records.Count - 1)
        {
            if (records[i].Speed > stationarySpeedThreshold)
            {
                i++;
                continue;
            }

            int startIdx = i;
            while (i < records.Count - 1 && records[i + 1].FuelLevel > records[i].FuelLevel)
            {
                i++;
            }

            int peakIdx = i;
            while (i < records.Count - 1 && records[i + 1].FuelLevel <= records[i].FuelLevel)
            {
                i++;
            }

            double startFuel = records[startIdx].FuelLevel;
            double peakFuel = records[peakIdx].FuelLevel;
            double fuelDiff = peakFuel - startFuel;

            if (fuelDiff < undulationThreshold)
            {
                continue;
            }

            bool isStableBefore = startIdx >= stabilityWindow && IsStable(records, startIdx - stabilityWindow, startIdx, undulationThreshold);
            bool isStableAfter = peakIdx + stabilityWindow < records.Count && IsStable(records, peakIdx, peakIdx + stabilityWindow, undulationThreshold);

            if (startFuel > 0 && fuelDiff >= minRefuelThreshold && fuelDiff <= maxRefuelThreshold && isStableBefore && isStableAfter)
            {
                report.AppendLine("<tr>");
                report.AppendLine($"<td style='border: 1px solid #ddd; padding: 8px;'>{assetCode}</td>");
                report.AppendLine($"<td style='border: 1px solid #ddd; padding: 8px;'>{jobCode}</td>");
                report.AppendLine($"<td style='border: 1px solid #ddd; padding: 8px;'>{records[startIdx].Rtc:yyyy-MM-dd HH:mm:ss}</td>");
                report.AppendLine($"<td style='border: 1px solid #ddd; padding: 8px;'>{records[peakIdx].Rtc:yyyy-MM-dd HH:mm:ss}</td>");
                report.AppendLine($"<td style='border: 1px solid #ddd; padding: 8px;'>{startFuel:F2}</td>");
                report.AppendLine($"<td style='border: 1px solid #ddd; padding: 8px;'>{peakFuel:F2}</td>");
                report.AppendLine($"<td style='border: 1px solid #ddd; padding: 8px;'>{fuelDiff:F2}</td>");
                report.AppendLine("<td style='border: 1px solid #ddd; padding: 8px;'>Refuel</td>");
                report.AppendLine("</tr>");
                refuelTotal += fuelDiff;
            }
        }

        return (report.ToString(), refuelTotal);
    }

    static bool IsStable(List<FuelRecord> records, int startIdx, int endIdx, double threshold)
    {
        double minFuel = records[startIdx].FuelLevel;
        double maxFuel = records[startIdx].FuelLevel;

        for (int i = startIdx; i <= endIdx; i++)
        {
            minFuel = Math.Min(minFuel, records[i].FuelLevel);
            maxFuel = Math.Max(maxFuel, records[i].FuelLevel);
        }

        return (maxFuel - minFuel) <= threshold;
    }

    // Implement SendEmailReportAsync using L&T Safety Application Email API
    static async Task SendEmailReportAsync(string jobCode, string emailBody, List<string> recipients, List<string> ccEmails)
    {
        const string apiEndpoint = "https://assetinsight.lntecc.com/safetyapplication/api/Email/GetMailAddress";
        const string username = "LNTNxTEm@ilAgentAdmin";
        const string password = "B4c9G5vtfsMAw@";

        using var httpClient = new HttpClient();

        var tasks = recipients.Select(async recipient =>
        {
            try
            {
                var payload = new
                {
                    Username = username,
                    Password = password,
                    EmailTo = recipient,
                    EMailCC = "subhamcodefreak@gmail.com", // Use the ccEmails parameter directly
                    EmailAttachments = new object[0],
                    EmailSubject = $"refuel - {jobCode}",
                    EMailBody = emailBody,
                    Application = "AIS",
                    ModuleName = "CVT-Fuel",
                    Priority = 2
                };

                var jsonPayload = JsonSerializer.Serialize(payload);
                var content = new StringContent(jsonPayload, System.Text.Encoding.UTF8, "application/json");

                var response = await httpClient.PostAsync(apiEndpoint, content);

                if (response.IsSuccessStatusCode)
                {
                    var responseContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Email sent successfully to {recipient}. Response: {responseContent}");
                }
                else
                {
                    Console.WriteLine($"Failed to send email to {recipient}. Status Code: {response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error sending email to {recipient}: {ex.Message}");
            }
        });

        await Task.WhenAll(tasks);
    }

    static List<string> LoadAssetCodesFromJson(string filePath)
    {
        if (!File.Exists(filePath))
        {
            Console.WriteLine($"File not found: {filePath}");
            return new List<string>();
        }

        try
        {
            var json = File.ReadAllText(filePath);
            var assetData = JsonSerializer.Deserialize<Dictionary<string, List<Dictionary<string, string>>>>(json);
            if (assetData == null || !assetData.ContainsKey("select dl.asset_code from d_lntassetmaster dl \r\n"))
            {
                Console.WriteLine("Invalid JSON structure.");
                return new List<string>();
            }

            var assetCodes = new List<string>();
            foreach (var item in assetData["select dl.asset_code from d_lntassetmaster dl \r\n"])
            {
                if (item.ContainsKey("asset_code"))
                {
                    assetCodes.Add(item["asset_code"]);
                }
            }

            return assetCodes;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading JSON file: {ex.Message}");
            return new List<string>();
        }
    }

    // Add a method to check if an asset has sensors installed
    static bool HasSensorsInstalled(string connString, string assetCode)
    {
        using var conn = new NpgsqlConnection(connString);
        conn.Open();
        using var cmd = new NpgsqlCommand("SELECT COUNT(*) FROM \"Fuel_Prod\".d_asset_calibration dac WHERE dac.assetcode = @assetCode", conn);
        cmd.Parameters.AddWithValue("assetCode", assetCode);
        return Convert.ToInt32(cmd.ExecuteScalar()) > 0;
    }

    // Add functionality to search for a specific asset code and display refuel data
    static void SearchAssetCodeAndDisplayRefuelData(string connString, string assetCode, DateTime startTime, DateTime endTime)
    {
        Console.WriteLine($"\n--- Searching for AssetCode: {assetCode} ---");

        if (!TryGetDeviceId(connString, assetCode, out int deviceId, out string? jobCode))
        {
            Console.WriteLine($"No device found for asset code {assetCode}.");
            return;
        }

        jobCode ??= "Unknown Job Code"; // Fallback value for jobCode

        var records = FetchFuelRecords(connString, deviceId, startTime, endTime);
        Console.WriteLine($"Fetched {records.Count} records for asset code {assetCode}.");
        if (records.Count == 0)
        {
            Console.WriteLine($"No records found for asset code {assetCode}.");
            return;
        }

        Console.WriteLine($"Device ID:{deviceId}");
        Console.WriteLine($"Job Code: {jobCode}");
        Console.WriteLine("---------------------------------------------------------------------------------------------------");
        Console.WriteLine($"| {"Start Time",-20} | {"End Time",-20} | {"Start Value",-12} | {"End Value",-12} | {"Amount",-10} | {"Type",-8} |");
        Console.WriteLine("---------------------------------------------------------------------------------------------------");

        var (report, totalRefueled) = AnalyzeFuelData(records, jobCode, assetCode);
        Console.WriteLine(report);
        Console.WriteLine($"Total Refueled: {totalRefueled:F2} L");
    }
}
