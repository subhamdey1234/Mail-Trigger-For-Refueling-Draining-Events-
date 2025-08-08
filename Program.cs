using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Mail;
using System.Text.Json;
using Npgsql;
using System.Text;

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
    static void Main(string[] args)
    {
        try
        {
            var appSettings = LoadAppSettings();
            if (appSettings == null || string.IsNullOrEmpty(appSettings.ConnectionStrings?.CitusDb)) return;

            var testData = appSettings.TestData;
            if (testData?.AssetCodes == null || testData.AssetCodes.Count == 0)
            {
                Console.WriteLine("No test asset codes found.");
                return;
            }

            if (!DateTime.TryParseExact(testData.StartTime, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime startTime) ||
                !DateTime.TryParseExact(testData.EndTime, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime endTime))
            {
                Console.WriteLine("Invalid test start/end time format.");
                return;
            }

            StringBuilder allReports = new StringBuilder();
            foreach (var assetcode in testData.AssetCodes)
            {
                Console.WriteLine($"\n--- Processing AssetCode: {assetcode} ---");
                if (!TryGetDeviceId(appSettings.ConnectionStrings.CitusDb, assetcode, out int deviceId, out string jobcode))
                {
                    Console.WriteLine($"No device found for assetcode {assetcode}.");
                    continue;
                }

                var records = FetchFuelRecords(appSettings.ConnectionStrings.CitusDb, deviceId, startTime, endTime);
                Console.WriteLine($"Fetched {records.Count} records for assetcode {assetcode}.");
                if (records.Count == 0)
                {
                    Console.WriteLine($"No records found for assetcode {assetcode}.");
                    continue;
                }

                string report = AnalyzeFuelData(records, assetcode ?? jobcode);
                Console.WriteLine(report);
                allReports.AppendLine(report);
            }
            // Print all reports first, then send email
            Console.WriteLine("\n--- Sending Email with All Reports ---\n");
            if (appSettings.Email == null || string.IsNullOrEmpty(appSettings.Email.Sender) || string.IsNullOrEmpty(appSettings.Email.AppPassword) || appSettings.Email.Recipients == null || appSettings.Email.Recipients.Count == 0)
            {
                Console.WriteLine("Email settings missing or incomplete in appsettings.json. Email not sent.");
            }
            else
            {
                SendEmailReport(testData.AssetCodes.Count == 1 ? testData.AssetCodes[0] : $"{testData.AssetCodes.Count} Assets", allReports.ToString(), appSettings.Email);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }

    static AppSettings LoadAppSettings()
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

    static bool TryGetDeviceId(string connString, string assetcode, out int deviceId, out string jobcode)
    {
        deviceId = 0;
        jobcode = null;
        using var conn = new NpgsqlConnection(connString);
        conn.Open();
        using var cmd = new NpgsqlCommand("SELECT platform_asset_id, jobcode FROM \"Fuel_Prod\".d_lntassetmaster WHERE asset_code = @assetcode", conn);
        cmd.Parameters.AddWithValue("assetcode", assetcode);
        using var reader = cmd.ExecuteReader();
        if (reader.Read())
        {
            deviceId = reader.GetInt32(0);
            jobcode = reader.IsDBNull(1) ? null : reader.GetString(1);
            return true;
        }
        return false;
    }

    static List<FuelRecord> FetchFuelRecords(string connString, int deviceId, DateTime startTime, DateTime endTime)
    {
        var list = new List<FuelRecord>();
        using var conn = new NpgsqlConnection(connString);
        conn.Open();
        using var cmd = new NpgsqlCommand("SELECT deviceid, fuel_level, rtc, tag_386, tag_387, tag_388 FROM \"Fuel_Prod\".m_fuel_metric_calc WHERE deviceid = @deviceid AND rtc BETWEEN @start AND @end ORDER BY rtc ASC", conn);
        cmd.Parameters.AddWithValue("deviceid", deviceId);
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

    static string AnalyzeFuelData(List<FuelRecord> records, string idLabel)
    {
        StringBuilder report = new();
        double refuelTotal = 0;
        const double minRefuelThreshold = 20.0;
        const double maxRefuelThreshold = 700.0;
        const double stationarySpeedThreshold = 0.5; // speed < 0.5 means stationary

        if (records == null || records.Count == 0)
        {
            report.AppendLine("No fuel records to analyze.");
            return report.ToString();
        }

        report.AppendLine($"Device ID: {records[0].DeviceId}");
        report.AppendLine($"Asset/Job Code: {idLabel}");
        report.AppendLine("---------------------------------------------------------------------------------------------------");
        report.AppendLine($"| {"Start Time",-20} | {"End Time",-20} | {"Start Value",-12} | {"End Value",-12} | {"Amount",-10} | {"Type",-8} |");
        report.AppendLine("---------------------------------------------------------------------------------------------------");

        int i = 0;
        bool foundRefuel = false;

        while (i < records.Count - 1)
        {
            // Skip non-stationary periods
            if (records[i].Speed > stationarySpeedThreshold)
            {
                i++;
                continue;
            }

            // Detect increasing sequence
            int startIdx = i;
            while (i < records.Count - 1 && records[i + 1].FuelLevel > records[i].FuelLevel)
            {
                i++;
            }

            // Detect peak and subsequent decrease
            int peakIdx = i;
            while (i < records.Count - 1 && records[i + 1].FuelLevel <= records[i].FuelLevel)
            {
                i++;
            }

            // Calculate refuel amount
            double startFuel = records[startIdx].FuelLevel;
            double peakFuel = records[peakIdx].FuelLevel;
            double fuelDiff = peakFuel - startFuel;

            // Exclude cases where start fuel level is zero
            if (startFuel > 0 && fuelDiff >= minRefuelThreshold && fuelDiff <= maxRefuelThreshold)
            {
                report.AppendLine($"| {records[startIdx].Rtc:yyyy-MM-dd HH:mm:ss} | {records[peakIdx].Rtc:yyyy-MM-dd HH:mm:ss} | {startFuel,12:F2} | {peakFuel,12:F2} | {fuelDiff,10:F2} | {"Refuel",-8} |");
                refuelTotal += fuelDiff;
                foundRefuel = true;
            }
        }

        if (!foundRefuel)
        {
            report.AppendLine("No refuel events detected in this period.");
        }

        report.AppendLine("---------------------------------------------------------------------------------------------------");
        report.AppendLine();
        report.AppendLine($"Total Refueled: {refuelTotal:F2} L");
        return report.ToString();
    }



    static void SendEmailReport(string subjectId, string body, EmailSettings emailSettings)
    {
    if (emailSettings == null || string.IsNullOrEmpty(emailSettings.Sender) || string.IsNullOrEmpty(emailSettings.AppPassword) || emailSettings.Recipients == null || emailSettings.Recipients.Count == 0)
    {
        Console.WriteLine("Email settings missing or incomplete. Email not sent.");
        return;
    }
    string sender = emailSettings.Sender;
    string password = emailSettings.AppPassword;
    var recipients = emailSettings.Recipients;
    try
    {
        foreach (var recipient in recipients)
        {
            if (string.IsNullOrEmpty(recipient))
            {
                Console.WriteLine("Recipient email is missing or empty. Skipping.");
                continue;
            }
            var msg = new MailMessage(sender, recipient, $"Fuel Report - {subjectId}", body);
            var smtp = new SmtpClient("smtp.gmail.com", 587)
            {
                EnableSsl = true,
                Credentials = new System.Net.NetworkCredential(sender, password)
            };
            smtp.Send(msg);
            Console.WriteLine($"Email sent successfully to {recipient}.");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Email failed: {ex.Message}");
    }
    }
}
