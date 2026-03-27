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
using DocumentFormat.OpenXml.Drawing.Diagrams;
using ClosedXML.Excel;

class ConnectionStrings
{
    public string? CitusDb { get; set; }
}
// End of Program class


class KalmanFilter
{
    private double _estimate; // Estimated state (fuel level)
    private double _errorEstimate; // Error in the estimate
    private readonly double _processNoise; // Process noise (variance in the system)
    private readonly double _measurementNoise; // Measurement noise (variance in the sensor)

    public KalmanFilter(double initialEstimate, double initialErrorEstimate, double processNoise, double measurementNoise)
    {
        _estimate = initialEstimate;
        _errorEstimate = initialErrorEstimate;
        _processNoise = processNoise;
        _measurementNoise = measurementNoise;
    }

    public double Update(double measurement)
    {
        // Prediction step
        _errorEstimate += _processNoise;

        // Kalman gain
        double kalmanGain = _errorEstimate / (_errorEstimate + _measurementNoise);

        // Update step
        _estimate = _estimate + kalmanGain * (measurement - _estimate);
        _errorEstimate = (1 - kalmanGain) * _errorEstimate;

        return _estimate;
    }
}


class TestData
{
    public List<string>? AssetCodes { get; set; }
    public string? StartTime { get; set; }
    public string? EndTime { get; set; }
    public string? BearerToken { get; set; }
    public string? ApiUrljobcode { get; set; }
    public string? ApiUrlassetCode { get; set; }
    public string? LoginAPIlink { get; set; }

    public string? FuelLevelAssets { get; set; }
}

class EmailSettings
{
    public string? Sender { get; set; }
    public string? AppPassword { get; set; }
    public List<string>? Recipients { get; set; }
    public List<string>? CCEmails { get; set; }
    public List<string>?  MainRecipient { get; set; }
}

class loginCredentials
{
    public string? Username { get; set; }
    public string? Password { get; set; }
}

class AppSettings
{
    public ConnectionStrings? ConnectionStrings { get; set; }

    public TestData? TestData { get; set; }


    public EmailSettings? Email { get; set; }

    public loginCredentials? loginCredentials{ get; set; }
}

class FuelRecord
{
    public object? DeviceId;
    public double FuelLevel;
    public DateTime Rtc;
    public double Latitude;
    public double Longitude;
    public double Speed;
    public string? AssetTypeName;

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

            if (string.IsNullOrEmpty(appSettings.TestData?.LoginAPIlink))
            {
                Console.WriteLine("Login API link is missing in appsettings.");
                return;
            }

            // Test GetTokenFromLogin
            string? token = await GetTokenFromLogin(appSettings.TestData.LoginAPIlink,appSettings);
            if (!string.IsNullOrEmpty(token))
            {
                Console.WriteLine($"Fetched Token: {token}");
            }
            else
            {
                Console.WriteLine("Failed to fetch token.");
            }

            if (args.Length == 0)
            {
                Console.WriteLine("Please specify a mode: 1 or 2");
                return;
            }

            switch (args[0])
            {
                case "1":
                    // Mode 1: Trigger all emails (existing functionality)
                    await TriggerAllEmails(appSettings);
                    break;

                case "2":
                    {
                        // Mode 2: Use database asset codes, compute refuel events and send email (do not modify Mode 1)
                        // Get main recipients from appsettings
                        if (appSettings.Email?.MainRecipient == null || !appSettings.Email.MainRecipient.Any())
                        {
                            Console.WriteLine("MainRecipient list is missing or empty in appsettings.json");
                            return;
                        }

                        var mode2StartStr = appSettings.TestData?.StartTime;
                        var mode2EndStr = appSettings.TestData?.EndTime;

                        if (!DateTime.TryParseExact(mode2StartStr, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime mode2Start) ||
                            !DateTime.TryParseExact(mode2EndStr, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime mode2End))
                        {
                            Console.WriteLine("Invalid start or end time format.");
                            return;
                        }

                        // Get asset codes for specific IC names
                        var specificAssetCodes = GetAssetCodesForSpecificIC(appSettings.ConnectionStrings.CitusDb);
                        if (specificAssetCodes.Count == 0)
                        {
                            Console.WriteLine("No asset codes found for the specified IC names.");
                            return;
                        }

                        Console.WriteLine($"Found {specificAssetCodes.Count} asset codes for specified IC names");

                        var emailBodyBuilder = new StringBuilder();
                        emailBodyBuilder.AppendLine("<html><body>");
                        emailBodyBuilder.AppendLine("<h3>Fuel Report - Water & Effluent Treatment IC and Larsen & Toubro Limited – PCIPL JV</h3>");
                        emailBodyBuilder.AppendLine("<table style='border: 1px solid #ddd; border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;'>");
                        emailBodyBuilder.AppendLine("<thead style='background-color: #f4f4f4;'><tr>");
                        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>ASSET_CODE</th>");
                        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>JOBCODE</th>");
                        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>START_TIME</th>");
                        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>END_TIME</th>");
                        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>START_VALUE</th>");
                        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>END_VALUE</th>");
                        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>Refuel Quantity</th>");
                        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>TYPE</th>");
                        emailBodyBuilder.AppendLine("</tr></thead>");
                        emailBodyBuilder.AppendLine("<tbody>");

                        int processedCount = 0;
                        int refuelCount = 0;

                        foreach (var assetCode in specificAssetCodes)
                        {
                            if (!TryGetDeviceId(appSettings.ConnectionStrings.CitusDb, assetCode, out int deviceId, out string? jobCode))
                                continue;

                            var records = FetchFuelRecords(appSettings.ConnectionStrings.CitusDb, deviceId, mode2Start, mode2End);

                            if (records == null || records.Count == 0)
                            {
                                processedCount++;
                                continue;
                            }

                            var (report, totalRefueled) = AnalyzeFuelData(records, jobCode ?? "Unknown Job Code", assetCode, mode2End);

                            if (totalRefueled > 0)
                            {
                                emailBodyBuilder.AppendLine(report);
                                refuelCount++;
                            }

                            processedCount++;
                            if (processedCount % 10 == 0)
                            {
                                Console.WriteLine($"Processed {processedCount}/{specificAssetCodes.Count} assets, found {refuelCount} refueling events");
                            }
                        }

                        emailBodyBuilder.AppendLine("</tbody></table>");
                        emailBodyBuilder.AppendLine($"<p>Total assets processed: {processedCount}</p>");
                        emailBodyBuilder.AppendLine($"<p>Total assets with refueling events: {refuelCount}</p>");
                        emailBodyBuilder.AppendLine("</body></html>");

                        if (refuelCount > 0)
                        {
                            await SendEmailReportAsync("Fuel Report - WET IC and PCIPL JV", emailBodyBuilder.ToString(), appSettings.Email.MainRecipient, null);
                            Console.WriteLine($"Email sent to {string.Join(", ", appSettings.Email.MainRecipient)} with {refuelCount} refueling events.");
                        }
                        else
                        {
                            Console.WriteLine("No refueling events found for the given period.");
                        }
                        break;
                    }

                case "3":
                    RunSearchMode(appSettings);
                    break;

                case "4":
                    await RunMode4SbgIcReport(appSettings);
                    break;

                case "5":
                    await RunMode5DrainDetection(appSettings);
                    break;

                default:
                    Console.WriteLine("Invalid mode. Please specify 1, 2, 3, 4, or 5.");
                    break;
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




    // Call the existing function
    static void RunSearchMode(AppSettings? appSettings)
    {
        if (appSettings == null || string.IsNullOrEmpty(appSettings.ConnectionStrings?.CitusDb))
        {
            Console.WriteLine("Database connection string is missing in appsettings.");
            return;
        }

        string connString = appSettings.ConnectionStrings.CitusDb!;

        // Determine default asset codes from appsettings (if any)
        string? defaultAssets = appSettings.TestData?.AssetCodes != null && appSettings.TestData.AssetCodes.Count > 0
            ? string.Join(",", appSettings.TestData.AssetCodes)
            : null;

        Console.Write($"Enter asset codes (comma-separated, e.g. 40550JYH,LTPP40677549H){(defaultAssets != null ? $" [default: from appsettings]" : "")}: ");
        string? assetInput = Console.ReadLine()?.Trim();

        List<string> assetCodes;
        if (string.IsNullOrEmpty(assetInput))
        {
            if (appSettings.TestData?.AssetCodes != null && appSettings.TestData.AssetCodes.Count > 0)
            {
                assetCodes = appSettings.TestData.AssetCodes;
            }
            else
            {
                Console.WriteLine("Asset codes are required.");
                return;
            }
        }
        else
        {
            assetCodes = assetInput.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
        }

        if (assetCodes.Count == 0)
        {
            Console.WriteLine("No valid asset codes provided.");
            return;
        }

        Console.WriteLine($"Total asset codes to process: {assetCodes.Count}");

        // Get start/end strings from appsettings or fallback to prompting
        string? startStr = appSettings.TestData?.StartTime;
        string? endStr = appSettings.TestData?.EndTime;

        if (string.IsNullOrWhiteSpace(startStr))
        {
            Console.Write("Enter start time (yyyy-MM-dd HH:mm:ss): ");
            startStr = Console.ReadLine()?.Trim();
        }
        else
        {
            Console.WriteLine($"Using start time from appsettings: {startStr}");
        }

        if (string.IsNullOrWhiteSpace(endStr))
        {
            Console.Write("Enter end time (yyyy-MM-dd HH:mm:ss): ");
            endStr = Console.ReadLine()?.Trim();
        }
        else
        {
            Console.WriteLine($"Using end time from appsettings: {endStr}");
        }

        if (!DateTime.TryParseExact(startStr, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime startTime))
        {
            Console.WriteLine("Invalid start time format. Use yyyy-MM-dd HH:mm:ss");
            return;
        }

        if (!DateTime.TryParseExact(endStr, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime endTime))
        {
            Console.WriteLine("Invalid end time format. Use yyyy-MM-dd HH:mm:ss");
            return;
        }

        // Iterate over each asset code and call the search function
        foreach (var assetCode in assetCodes)
        {
            SearchAssetCodeAndDisplayRefuelData(connString, assetCode, startTime, endTime);
        }
    }

    static async Task RunMode4SbgIcReport(AppSettings appSettings)
    {
        if (appSettings.ConnectionStrings?.CitusDb == null)
        {
            Console.WriteLine("Database connection string is missing.");
            return;
        }

        if (appSettings.Email?.MainRecipient == null || !appSettings.Email.MainRecipient.Any())
        {
            Console.WriteLine("MainRecipient list is missing or empty in appsettings.json");
            return;
        }

        var mode4StartStr = appSettings.TestData?.StartTime;
        var mode4EndStr = appSettings.TestData?.EndTime;

        if (!DateTime.TryParseExact(mode4StartStr, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime mode4Start) ||
            !DateTime.TryParseExact(mode4EndStr, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime mode4End))
        {
            Console.WriteLine("Invalid start or end time format in appsettings.");
            return;
        }

        var assetCodes = GetAssetCodesForSbgAndIc(appSettings.ConnectionStrings.CitusDb);
        if (assetCodes.Count == 0)
        {
            Console.WriteLine("No asset codes found for the specified SBG + IC filter.");
            return;
        }

        var emailBodyBuilder = new StringBuilder();
        emailBodyBuilder.AppendLine("<html><body>");
        emailBodyBuilder.AppendLine("<h3>Fuel Report - SBG & IC Filtered Assets</h3>");
        emailBodyBuilder.AppendLine("<table style='border: 1px solid #ddd; border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;'>");
        emailBodyBuilder.AppendLine("<thead style='background-color: #f4f4f4;'><tr>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>ASSET_CODE</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>JOBCODE</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>START_TIME</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>END_TIME</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>START_VALUE</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>END_VALUE</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>Refuel Quantity</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>TYPE</th>");
        emailBodyBuilder.AppendLine("</tr></thead>");
        emailBodyBuilder.AppendLine("<tbody>");

        int processedCount = 0;
        int refuelCount = 0;

        foreach (var assetCode in assetCodes)
        {
            if (!TryGetDeviceId(appSettings.ConnectionStrings.CitusDb, assetCode, out int deviceId, out string? jobCode))
                continue;

            var records = FetchFuelRecords(appSettings.ConnectionStrings.CitusDb, deviceId, mode4Start, mode4End);

            if (records == null || records.Count == 0)
            {
                processedCount++;
                continue;
            }

            var (report, totalRefueled) = AnalyzeFuelData(records, jobCode ?? "Unknown Job Code", assetCode, mode4End);

            if (totalRefueled > 0)
            {
                emailBodyBuilder.AppendLine(report);
                refuelCount++;
            }

            processedCount++;
            if (processedCount % 10 == 0)
            {
                Console.WriteLine($"[Mode 4] Processed {processedCount}/{assetCodes.Count} assets, found {refuelCount} refueling events");
            }
        }

        emailBodyBuilder.AppendLine("</tbody></table>");
        emailBodyBuilder.AppendLine($"<p>Total assets processed: {processedCount}</p>");
        emailBodyBuilder.AppendLine($"<p>Total assets with refueling events: {refuelCount}</p>");
        emailBodyBuilder.AppendLine("</body></html>");

        Console.WriteLine($"[Mode 4] Done. Processed {processedCount} assets, {refuelCount} with refueling events.");

        if (refuelCount > 0)
        {
            await SendEmailReportAsync("Fuel Report - SBG & IC Filtered", emailBodyBuilder.ToString(), appSettings.Email.MainRecipient, appSettings.Email.CCEmails);
            Console.WriteLine($"[Mode 4] Email sent to {string.Join(", ", appSettings.Email.MainRecipient)} with {refuelCount} refueling events.");
        }
        else
        {
            Console.WriteLine("[Mode 4] No refueling events found for the given period.");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  MODE 5: Fuel Drain Detection — detects sudden fuel decrease events
    //
    //  Logic:
    //  - Uses a 5–10 minute sliding window over fuel telemetry data.
    //  - If fuel level drops > 10 L within that window, it is flagged as a drain.
    //  - Undulation / sensor noise (small oscillations) is filtered out using
    //    median-3 smoothing; only sustained decreases are considered.
    //  - Normal gradual consumption (e.g. 102→101→99→98) is NOT treated as drain.
    //    A drain requires a SUDDEN drop (e.g. 102→101→99→88 within 5 min).
    //  - Sends an email alert with all detected drain events.
    // ═══════════════════════════════════════════════════════════════════════════
    static async Task RunMode5DrainDetection(AppSettings appSettings)
    {
        if (appSettings.ConnectionStrings?.CitusDb == null)
        {
            Console.WriteLine("[Mode 5] Database connection string is missing.");
            return;
        }

        if (appSettings.Email?.MainRecipient == null || !appSettings.Email.MainRecipient.Any())
        {
            Console.WriteLine("[Mode 5] MainRecipient list is missing or empty in appsettings.json");
            return;
        }

        var startStr = appSettings.TestData?.StartTime;
        var endStr = appSettings.TestData?.EndTime;

        if (!DateTime.TryParseExact(startStr, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime startTime) ||
            !DateTime.TryParseExact(endStr, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime endTime))
        {
            Console.WriteLine("[Mode 5] Invalid start or end time format in appsettings.");
            return;
        }

        var assetCodes = GetAssetCodesForSbgAndIc(appSettings.ConnectionStrings.CitusDb);
        if (assetCodes.Count == 0)
        {
            Console.WriteLine("[Mode 5] No asset codes found for the specified SBG + IC filter.");
            return;
        }

        Console.WriteLine($"[Mode 5 - Drain Detection] Found {assetCodes.Count} asset codes. Processing...");

        var emailBodyBuilder = new StringBuilder();
        emailBodyBuilder.AppendLine("<html><body>");
        emailBodyBuilder.AppendLine("<h3 style='color:#c0392b;'>⚠ Fuel Drain Detection Report</h3>");
        emailBodyBuilder.AppendLine($"<p>Report Period: {startTime:yyyy-MM-dd HH:mm:ss} to {endTime:yyyy-MM-dd HH:mm:ss}</p>");
        emailBodyBuilder.AppendLine("<table style='border: 1px solid #ddd; border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;'>");
        emailBodyBuilder.AppendLine("<thead style='background-color: #e74c3c; color: white;'><tr>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>ASSET_CODE</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>JOBCODE</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>DRAIN_START_TIME</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>DRAIN_END_TIME</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>START_LEVEL (L)</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>END_LEVEL (L)</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>DRAIN_QUANTITY (L)</th>");
        emailBodyBuilder.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>DURATION (min)</th>");
        emailBodyBuilder.AppendLine("</tr></thead>");
        emailBodyBuilder.AppendLine("<tbody>");

        int processedCount = 0;
        int drainEventCount = 0;

        foreach (var assetCode in assetCodes)
        {
            if (!TryGetDeviceId(appSettings.ConnectionStrings.CitusDb, assetCode, out int deviceId, out string? jobCode))
                continue;

            var records = FetchFuelRecords(appSettings.ConnectionStrings.CitusDb, deviceId, startTime, endTime);

            if (records == null || records.Count == 0)
            {
                processedCount++;
                continue;
            }

            var drainEvents = DetectFuelDrainEvents(records);

            foreach (var drain in drainEvents)
            {
                double durationMin = (drain.EndTime - drain.StartTime).TotalMinutes;
                emailBodyBuilder.AppendLine("<tr style='background-color:#fdedec;'>");
                emailBodyBuilder.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{assetCode}</td>");
                emailBodyBuilder.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{jobCode ?? "Unknown"}</td>");
                emailBodyBuilder.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{drain.StartTime:yyyy-MM-dd HH:mm:ss}</td>");
                emailBodyBuilder.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{drain.EndTime:yyyy-MM-dd HH:mm:ss}</td>");
                emailBodyBuilder.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{drain.StartLevel:F2}</td>");
                emailBodyBuilder.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{drain.EndLevel:F2}</td>");
                emailBodyBuilder.AppendLine($"<td style='border:1px solid #ddd;padding:8px;color:#c0392b;font-weight:bold'>{drain.DrainQuantity:F2}</td>");
                emailBodyBuilder.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{durationMin:F1}</td>");
                emailBodyBuilder.AppendLine("</tr>");
                drainEventCount++;

                Console.WriteLine(
                    $"[DRAIN] Asset={assetCode} | " +
                    $"{drain.StartTime:yyyy-MM-dd HH:mm:ss} -> {drain.EndTime:yyyy-MM-dd HH:mm:ss} | " +
                    $"Level: {drain.StartLevel:F2}L -> {drain.EndLevel:F2}L | " +
                    $"Drained={drain.DrainQuantity:F2}L in {durationMin:F1}min");
            }

            processedCount++;
            if (processedCount % 10 == 0)
            {
                Console.WriteLine($"[Mode 5] Processed {processedCount}/{assetCodes.Count} assets, found {drainEventCount} drain events");
            }
        }

        emailBodyBuilder.AppendLine("</tbody></table>");
        emailBodyBuilder.AppendLine($"<p>Total assets processed: {processedCount}</p>");
        emailBodyBuilder.AppendLine($"<p style='color:#c0392b;font-weight:bold;'>Total drain events detected: {drainEventCount}</p>");
        emailBodyBuilder.AppendLine("</body></html>");

        Console.WriteLine($"[Mode 5] Done. Processed {processedCount} assets, {drainEventCount} drain events found.");

        if (drainEventCount > 0)
        {
            await SendEmailReportAsync(
                "⚠ ALERT: Fuel Drain Detection Report",
                emailBodyBuilder.ToString(),
                appSettings.Email.Recipients,
                appSettings.Email.CCEmails);
            Console.WriteLine($"[Mode 5] Drain alert email sent to {string.Join(", ", appSettings.Email.MainRecipient)}.");
        }
        else
        {
            Console.WriteLine("[Mode 5] No fuel drain events detected for the given period.");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Drain Detection Algorithm
    //
    //  Sliding window: 5 to 10 minutes.
    //  Threshold: fuel drops > 10 L within window → drain.
    //  Undulation filter: median-3 smoothing removes single-sample sensor spikes.
    //  Normal consumption filter: gradual decrease (small per-step drops spread
    //      across the window) is NOT flagged. Only a SUDDEN sharp drop is flagged.
    //      "Sudden" = the largest single-step drop (or sum of 2 consecutive steps)
    //      accounts for >60 % of the total window decrease.
    // ─────────────────────────────────────────────────────────────────────────
    static List<(DateTime StartTime, DateTime EndTime, double StartLevel, double EndLevel, double DrainQuantity)>
        DetectFuelDrainEvents(List<FuelRecord> records)
    {
        var drainEvents = new List<(DateTime, DateTime, double, double, double)>();

        if (records == null || records.Count < 3)
            return drainEvents;

        // Filter valid records and sort by time
        var recs = records
            .Where(r => r.FuelLevel > 0 && r.Rtc != DateTime.MinValue)
            .OrderBy(r => r.Rtc)
            .ToList();

        if (recs.Count < 3)
            return drainEvents;

        int n = recs.Count;

        // ── Median-3 smoothing to remove sensor spikes / undulation ──
        var sm = new double[n];
        for (int s = 0; s < n; s++)
        {
            int lo = Math.Max(0, s - 1);
            int hi = Math.Min(n - 1, s + 1);
            double a = recs[lo].FuelLevel, b = recs[s].FuelLevel, c = recs[hi].FuelLevel;
            sm[s] = a < b ? (b < c ? b : (a < c ? c : a))
                          : (a < c ? a : (b < c ? c : b));
        }

        // ── Thresholds ──
        const double minDrainThreshold = 10.0;     // minimum litres to flag as drain
        const double windowMinSeconds  = 5 * 60;   // 5 minutes
        const double windowMaxSeconds  = 10 * 60;  // 10 minutes
        const double suddenDropRatio   = 0.60;     // ≥60% of total drop in 1-2 steps → sudden

        // Track the end-time of the last accepted drain so we don't double-count
        DateTime lastDrainEnd = DateTime.MinValue;

        for (int i = 0; i < n; i++)
        {
            // Skip if this point is inside a previously detected drain window
            if (recs[i].Rtc <= lastDrainEnd)
                continue;

            // ── Find the farthest index j within the 10-min window ──
            int jMax = i;
            for (int k = i + 1; k < n; k++)
            {
                double elapsedSec = (recs[k].Rtc - recs[i].Rtc).TotalSeconds;
                if (elapsedSec > windowMaxSeconds) break;
                jMax = k;
            }

            if (jMax == i) continue;

            // ── Search for the best (largest) drain within [i .. jMax] ──
            // Walk forward and track the peak (highest smoothed level) and
            // lowest subsequent point within the window.
            double peakLevel = sm[i];
            int peakIdx = i;
            double bestDrain = 0;
            int bestDrainEndIdx = i;

            for (int j = i + 1; j <= jMax; j++)
            {
                if (sm[j] > peakLevel)
                {
                    peakLevel = sm[j];
                    peakIdx = j;
                }

                double drop = peakLevel - sm[j];
                if (drop > bestDrain)
                {
                    bestDrain = drop;
                    bestDrainEndIdx = j;
                }
            }

            // ── Gate 1: minimum drain quantity ──
            if (bestDrain < minDrainThreshold)
                continue;

            // ── Gate 2: must be within the 5–10 min window ──
            double durationSec = (recs[bestDrainEndIdx].Rtc - recs[peakIdx].Rtc).TotalSeconds;
            if (durationSec < windowMinSeconds || durationSec > windowMaxSeconds)
            {
                // Also check if the drain is just under 5 min but very large — still accept
                if (!(bestDrain >= minDrainThreshold * 2 && durationSec >= 60))
                    continue;
            }

            // ── Gate 3: Suddenness check — distinguish drain from normal consumption ──
            // Normal consumption: fuel drops slowly and uniformly across many steps.
            // Drain: one or two large step-drops dominate the total decrease.
            double maxSingleStepDrop = 0;
            double maxTwoConsecDrop = 0;
            double prevStepDrop = 0;

            for (int k = peakIdx + 1; k <= bestDrainEndIdx; k++)
            {
                double stepDrop = sm[k - 1] - sm[k]; // positive means fuel decreased
                if (stepDrop > maxSingleStepDrop)
                    maxSingleStepDrop = stepDrop;

                double twoStepDrop = stepDrop + prevStepDrop;
                if (twoStepDrop > maxTwoConsecDrop)
                    maxTwoConsecDrop = twoStepDrop;

                prevStepDrop = stepDrop;
            }

            double dominantDrop = Math.Max(maxSingleStepDrop, maxTwoConsecDrop);
            if (dominantDrop < bestDrain * suddenDropRatio)
            {
                // The decrease is spread uniformly — likely normal consumption, not drain
                Console.WriteLine(
                    $"[SKIP-NormalConsumption] " +
                    $"{recs[peakIdx].Rtc:HH:mm:ss}->{recs[bestDrainEndIdx].Rtc:HH:mm:ss} " +
                    $"Drop={bestDrain:F2}L but dominant step={dominantDrop:F2}L " +
                    $"({dominantDrop / bestDrain * 100:F0}% < {suddenDropRatio * 100}%) => normal consumption");
                continue;
            }

            // ── Gate 4: Undulation filter — if fuel bounces back up within undulationBand ──
            // Check a few records after the drain end; if fuel recovers significantly,
            // it was sensor noise, not a real drain.
            bool bouncedBack = false;
            int checkEnd = Math.Min(n - 1, bestDrainEndIdx + 5);
            for (int k = bestDrainEndIdx + 1; k <= checkEnd; k++)
            {
                double recovery = sm[k] - sm[bestDrainEndIdx];
                if (recovery > bestDrain * 0.50)
                {
                    bouncedBack = true;
                    break;
                }
            }
            if (bouncedBack)
            {
                Console.WriteLine(
                    $"[SKIP-Undulation] " +
                    $"{recs[peakIdx].Rtc:HH:mm:ss}->{recs[bestDrainEndIdx].Rtc:HH:mm:ss} " +
                    $"Drop={bestDrain:F2}L but fuel bounced back => sensor noise");
                continue;
            }

            // ── Gate 5: Post-drain sustained low level check ──
            // After a real drain, fuel should STAY low. Check that the average
            // level in the 3 minutes after drain end is still below start - 50% of drain.
            double postCheckSec = 180;
            var postVals = new List<double>();
            for (int k = bestDrainEndIdx; k < n; k++)
            {
                if ((recs[k].Rtc - recs[bestDrainEndIdx].Rtc).TotalSeconds > postCheckSec)
                    break;
                postVals.Add(sm[k]);
            }
            if (postVals.Count >= 2)
            {
                double postAvg = postVals.Average();
                if (postAvg > peakLevel - bestDrain * 0.50)
                {
                    Console.WriteLine(
                        $"[SKIP-NotSustained] " +
                        $"{recs[peakIdx].Rtc:HH:mm:ss}->{recs[bestDrainEndIdx].Rtc:HH:mm:ss} " +
                        $"Drop={bestDrain:F2}L but post-avg={postAvg:F2} (peak={peakLevel:F2}) => not sustained");
                    continue;
                }
            }

            // ═══ DRAIN EVENT CONFIRMED ═══
            double startLevel = recs[peakIdx].FuelLevel;
            double endLevel = recs[bestDrainEndIdx].FuelLevel;
            double drainQty = startLevel - endLevel;

            drainEvents.Add((
                recs[peakIdx].Rtc,
                recs[bestDrainEndIdx].Rtc,
                startLevel,
                endLevel,
                drainQty
            ));

            lastDrainEnd = recs[bestDrainEndIdx].Rtc;
            // Advance past the drain event
            i = bestDrainEndIdx;
        }

        return drainEvents;
    }


    static List<FuelRecord> FetchFuelRecords(string connString, int deviceId, DateTime startTime, DateTime endTime)
    {
        // Fetch 6 hours past endTime so cross-midnight refuels are captured.
        // The analysis function uses a reportCutoff to attribute events correctly.
        DateTime fetchEnd = endTime.AddHours(6);

        var list = new List<FuelRecord>();
        using var conn = new NpgsqlConnection(connString);
        conn.Open();
        using var cmd = new NpgsqlCommand(
                   @"SELECT 
                m.deviceid, 
                m.fuel_level, 
                m.rtc, 
                m.tag_386, 
                m.tag_387, 
                m.tag_388,
                d.assettypename
              FROM ""Fuel_Prod"".m_fuel_metric_calc_dist m
              LEFT JOIN ""Fuel_Prod"".d_lntassetmaster d 
                ON m.deviceid = d.platform_asset_id
              WHERE m.deviceid = @deviceId 
                AND m.rtc BETWEEN @start AND @end 
              ORDER BY m.rtc ASC",
                   conn);
        cmd.Parameters.AddWithValue("deviceId", deviceId);
        cmd.Parameters.AddWithValue("start", startTime);
        cmd.Parameters.AddWithValue("end", fetchEnd);
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
                Speed = reader.IsDBNull(reader.GetOrdinal("tag_388")) ? 0 : Convert.ToDouble(reader["tag_388"]),
                AssetTypeName= reader.IsDBNull(reader.GetOrdinal("assettypename"))? "Unknown" : reader.GetString(reader.GetOrdinal("assettypename"))
            });
        }
        return list;
    }

    static List<double> ApplyExponentialSmoothing(List<double> data, double alpha)
    {
        List<double> smoothedData = new();
        if (data.Count == 0) return smoothedData;

        smoothedData.Add(data[0]); // Initialize with the first data point
        for (int i = 1; i < data.Count; i++)
        {
            double smoothedValue = alpha * data[i] + (1 - alpha) * smoothedData[i - 1];
            smoothedData.Add(smoothedValue);
        }

        return smoothedData;
    }

    // Ensure only one table header in AnalyzeFuelData method
    //static (string report, double totalRefueled) AnalyzeFuelData(List<FuelRecord> records, string jobCode, string assetCode)
    //{
    //    StringBuilder report = new();
    //    double refuelTotal = 0;

    //    const double minRefuelThreshold = 10.0;  // Adjusted minimum fuel difference to consider as refuel
    //    const double maxRefuelThreshold = 1200.0; // Adjusted maximum fuel difference to consider as refuel
    //    const double stationarySpeedThreshold = 0.0; // Reduced speed threshold for stationary detection
    //    const double undulationThreshold = 4.0; // Reduced undulation threshold for better stability detection
    //    const int stabilityWindow = 5; // Increased stability window for more robust checks
    //    const int minRefuelDurationSeconds = 120; // Increased minimum duration for a refuel event

    //    if (records == null || records.Count == 0)
    //        return (string.Empty, refuelTotal);

    //    for (int i = 0; i < records.Count - 1; i++)
    //    {
    //        if (records[i].Speed > stationarySpeedThreshold || records[i].FuelLevel == 0)
    //            continue;

    //        int startIdx = i;
    //        while (i < records.Count - 1 && records[i + 1].FuelLevel > records[i].FuelLevel && records[i].Speed == stationarySpeedThreshold)
    //            i++;

    //        int peakIdx = i;
    //        double startFuel = records[startIdx].FuelLevel;
    //        double peakFuel = records[peakIdx].FuelLevel;
    //        double fuelDiff = peakFuel - startFuel;

    //        // Check if the fuel difference is within valid thresholds
    //        if (fuelDiff < minRefuelThreshold || fuelDiff > maxRefuelThreshold)
    //            continue;

    //        // Check if the duration of the refuel event is sufficient
    //        TimeSpan refuelDuration = records[peakIdx].Rtc - records[startIdx].Rtc;
    //        if (refuelDuration.TotalSeconds < minRefuelDurationSeconds)
    //            continue;

    //        // Check stability before and after the refuel event
    //        bool isStableBefore = startIdx >= stabilityWindow && IsStable(records, startIdx - stabilityWindow, startIdx, undulationThreshold);
    //        bool isStableAfter = peakIdx + stabilityWindow < records.Count && IsStable(records, peakIdx, peakIdx + stabilityWindow, undulationThreshold);

    //        // Log missing data or anomalies
    //        if (!isStableBefore || !isStableAfter)
    //        {
    //            Console.WriteLine($"Potential refuel detected for asset {assetCode} between {records[startIdx].Rtc} and {records[peakIdx].Rtc}. => {fuelDiff}");
    //            continue;
    //        }

    //        report.AppendLine("<tr>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{assetCode}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{jobCode}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{records[startIdx].Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{records[peakIdx].Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{startFuel:F2}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{peakFuel:F2}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{fuelDiff:F2}</td>");
    //        report.AppendLine("<td style='border:1px solid #ddd;padding:8px'>Refuel</td>");
    //        report.AppendLine("</tr>");

    //        refuelTotal += fuelDiff;
    //    }

    //    return (report.ToString(), refuelTotal);
    //}

    //         -----------------------------------------------------------------
    //--------------------------------------------------------------------------------------
    //--------------------------------------------------------------------------------------

    //static (string report, double totalRefueled) AnalyzeFuelData(
    //List<FuelRecord> records, string jobCode, string assetCode)
    //{
    //    StringBuilder report = new();
    //    double refuelTotal = 0;







    //    // --- Tunable thresholds ---
    //    const double minRefuelThreshold = 10.0;   // min net fuel gain to count as refuel
    //    const double maxRefuelThreshold = 1200.0; // max net fuel gain (sanity cap)
    //    const double maxSpeedDuringRefuel = 2.0;    // allow slight GPS drift (was 0.0)
    //    const double undulationThreshold = 4.0;    // stability band (relaxed slightly)
    //    const int stabilityWindow = 3;      // records to check for stability
    //    const int minRefuelDurationSec = 90;     // min seconds for a valid refuel (relaxed)
    //    const double sensorNoiseTolerance = 4.0;    // allow small mid-fill dips (noise)
    //    const double minRisePerStep = -sensorNoiseTolerance; // fuel can dip by this much mid-fill

    //    if (records == null || records.Count == 0)
    //        return (string.Empty, refuelTotal);

    //    int n = records.Count;

    //    for (int i = 0; i < n - 1; i++)
    //    {
    //        var rec = records[i];

    //        // Skip if moving too fast or no fuel reading
    //        if (rec.Speed > maxSpeedDuringRefuel || rec.FuelLevel <= 0)
    //            continue;

    //        int startIdx = i;
    //        double startFuel = rec.FuelLevel;

    //        // --- Walk forward while fuel is generally rising (allow small sensor dips) ---
    //        int peakIdx = startIdx;
    //        double peakFuel = startFuel;

    //        int j = startIdx + 1;
    //        while (j < n)
    //        {
    //            double fuelChange = records[j].FuelLevel - records[j - 1].FuelLevel;

    //            // Stop if speed picks up (asset is moving)
    //            if (records[j].Speed > maxSpeedDuringRefuel)
    //                break;

    //            // Stop if fuel drops significantly (not just noise)
    //            if (fuelChange < minRisePerStep)
    //                break;

    //            // Track the highest fuel level seen in this window
    //            if (records[j].FuelLevel > peakFuel)
    //            {
    //                peakFuel = records[j].FuelLevel;
    //                peakIdx = j;
    //            }

    //            j++;
    //        }

    //        // Move outer loop past the window we just scanned
    //        i = j - 1;

    //        double fuelDiff = peakFuel - startFuel;

    //        // --- Gate 1: net fuel gain must be within valid range ---
    //        if (fuelDiff < minRefuelThreshold || fuelDiff > maxRefuelThreshold)
    //            continue;

    //        // --- Gate 2: event must last long enough ---
    //        TimeSpan duration = records[peakIdx].Rtc - records[startIdx].Rtc;
    //        if (duration.TotalSeconds < minRefuelDurationSec)
    //            continue;

    //        // --- Gate 3: stability checks (warn but don't discard) ---
    //        bool isStableBefore = startIdx >= stabilityWindow &&
    //                              IsStable(records, startIdx - stabilityWindow, startIdx, undulationThreshold);
    //        bool isStableAfter = peakIdx + stabilityWindow < n &&
    //                              IsStable(records, peakIdx, peakIdx + stabilityWindow, undulationThreshold);

    //        if (!isStableBefore || !isStableAfter)
    //        {
    //            // Log as a LOW-CONFIDENCE refuel instead of silently dropping it
    //            Console.WriteLine(
    //                $"[LOW-CONFIDENCE] Asset={assetCode} | " +
    //                $"{records[startIdx].Rtc:yyyy-MM-dd HH:mm:ss} → {records[peakIdx].Rtc:yyyy-MM-dd HH:mm:ss} | " +
    //                $"Gain={fuelDiff:F2}L | StableBefore={isStableBefore} StableAfter={isStableAfter}");

    //            // Still capture it — mark differently in the report
    //            AppendRefuelRow(report, assetCode, jobCode, records[startIdx], records[peakIdx],
    //                            startFuel, peakFuel, fuelDiff, confidence: "Low");
    //        }
    //        else
    //        {
    //            AppendRefuelRow(report, assetCode, jobCode, records[startIdx], records[peakIdx],
    //                            startFuel, peakFuel, fuelDiff, confidence: "High");
    //        }

    //        refuelTotal += fuelDiff;
    //    }

    //    return (report.ToString(), refuelTotal);
    //}

    //// --- Helper: emit one HTML table row ---
    //static void AppendRefuelRow(
    //    StringBuilder report,
    //    string assetCode, string jobCode,
    //    FuelRecord start, FuelRecord peak,
    //    double startFuel, double peakFuel, double fuelDiff,
    //    string confidence)
    //{
    //    string rowStyle = confidence == "Low"
    //        ? "background-color:#fff3cd"   // amber tint for low-confidence rows
    //        : "";

    //    report.AppendLine($"<tr style='{rowStyle}'>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{assetCode}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{jobCode}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{start.Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{peak.Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{startFuel:F2}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{peakFuel:F2}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{fuelDiff:F2}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>Refuel ({confidence})</td>");
    //    report.AppendLine("</tr>");
    //}

    //// --- Helper: fuel level variance within a window ---
    //static bool IsStable(List<FuelRecord> records, int startIdx, int endIdx, double threshold)
    //{
    //    double min = records[startIdx].FuelLevel;
    //    double max = records[startIdx].FuelLevel;

    //    for (int i = startIdx + 1; i <= endIdx && i < records.Count; i++)
    //    {
    //        if (records[i].FuelLevel < min) min = records[i].FuelLevel;
    //        if (records[i].FuelLevel > max) max = records[i].FuelLevel;
    //    }

    //    return (max - min) <= threshold;
    //}

    //   -------------------------------------------------------------------------------------------
    //-------------------------------------------------------------------------------------
    //-----------------------------------------------------------------------------------------------


    //static (string report, double totalRefueled) AnalyzeFuelData(List<FuelRecord> records, string jobCode, string assetCode)
    //{
    //    // Improved pipeline:
    //    // 1. Validate and split sessions by large time gaps
    //    // 2. Smooth sensor using Kalman + median filter
    //    // 3. Detect refuel events by finding stable-before, rising, stable-after windows
    //    // 4. Use median of pre/post windows as start/end values to compute refuel amount

    //    StringBuilder report = new();
    //    double refuelTotal = 0;

    //    // Tunable thresholds
    //    const double minRefuelLiters = 10.0;
    //    const double maxRefuelLiters = 700.0;
    //    const double stationarySpeedThreshold = 0.0; // km/h
    //    const double undulationThreshold = 3.0; // allowed noise in fuel level when considered stable
    //    const int stabilityWindowSeconds =100; // how long before/after should be stable
    //    const int minRefuelDurationSeconds = 60;
    //    const double gpsVarianceThreshold = 0.0002; // approximate degrees variance allowed for GPS while stationary
    //    const int medianFilterWindow = 3;

    //    if (records == null || records.Count == 0)
    //        return (string.Empty, refuelTotal);

    //    // Remove obviously invalid records
    //    var validRecords = records
    //        .Where(r => r.Rtc != DateTime.MinValue && r.FuelLevel > 0)
    //        .OrderBy(r => r.Rtc)
    //        .ToList();

    //    if (validRecords.Count == 0)
    //        return (string.Empty, refuelTotal);

    //    // Split into sessions when there are big time gaps (e.g., > 30 minutes)
    //    var sessions = SplitByTimeGap(validRecords, TimeSpan.FromMinutes(30));

    //    foreach (var session in sessions)
    //    {
    //        if (session.Count < 3) continue;

    //        // Smooth values: Kalman then median filter
    //        double processNoise = 0.1;
    //        double measurementNoise = Math.Max(1.0, ComputeStdDev(session.Select(s => s.FuelLevel).ToList()));
    //        var kf = new KalmanFilter(session[0].FuelLevel, 1.0, processNoise, measurementNoise);

    //        var kalmanSmoothed = new List<double>(session.Count);
    //        foreach (var r in session)
    //        {
    //            kalmanSmoothed.Add(kf.Update(r.FuelLevel));
    //        }

    //        var smoothed = MedianFilter(kalmanSmoothed, medianFilterWindow);

    //        // scan for refuel events
    //        int i = 0;
    //        while (i < session.Count - 1)
    //        {
    //            // require current point to be stationary
    //            if (!IsStationary(session, i, stabilityWindowSeconds, stationarySpeedThreshold))
    //            {
    //                i++;
    //                continue;
    //            }

    //            // require stability before start
    //            int preWindowCount = CountSecondsWindow(session, i, stabilityWindowSeconds);
    //            if (preWindowCount < 1)
    //            {
    //                i++;
    //                continue;
    //            }

    //            // find rising sequence
    //            int startIdx = i;
    //            int j = i;
    //            bool risingFound = false;
    //            while (j < session.Count - 1)
    //            {
    //                // allow slight undulation but overall increasing trend
    //                if (smoothed[j + 1] - smoothed[j] > 0.1)
    //                {
    //                    risingFound = true;
    //                    j++;
    //                }
    //                else if (Math.Abs(smoothed[j + 1] - smoothed[j]) <= undulationThreshold * 0.5)
    //                {
    //                    // small plateau - treat as continuing if already rising
    //                    j++;
    //                }
    //                else break;
    //            }

    //            if (!risingFound)
    //            {
    //                i++;
    //                continue;
    //            }

    //            int peakIdx = j;

    //            // require stable after peak
    //            if (!IsStationary(session, peakIdx, stabilityWindowSeconds, stationarySpeedThreshold) ||
    //                !IsGpsStable(session, peakIdx, stabilityWindowSeconds, gpsVarianceThreshold))
    //            {
    //                i = peakIdx + 1;
    //                continue;
    //            }

    //            // compute median pre/post values using window length (clamped)
    //            int preStart = Math.Max(0, startIdx - Math.Max(1, stabilityWindowSeconds / 10));
    //            int preEnd = Math.Min(session.Count - 1, startIdx);
    //            int postStart = Math.Max(0, peakIdx);
    //            int postEnd = Math.Min(session.Count - 1, peakIdx + Math.Max(1, stabilityWindowSeconds / 10));

    //            var preValues = smoothed.GetRange(preStart, preEnd - preStart + 1);
    //            var postValues = smoothed.GetRange(postStart, postEnd - postStart + 1);

    //            double startMedian = Median(preValues);
    //            double endMedian = Median(postValues);
    //            double fuelDiff = endMedian - startMedian;

    //            TimeSpan duration = session[peakIdx].Rtc - session[startIdx].Rtc;

    //            // validations
    //            if (fuelDiff >= minRefuelLiters &&
    //                fuelDiff <= maxRefuelLiters &&
    //                duration.TotalSeconds >= minRefuelDurationSeconds &&
    //                IsStable(smoothed, preStart, preEnd, undulationThreshold) &&
    //                IsStable(smoothed, postStart, postEnd, undulationThreshold))
    //            {
    //                Console.WriteLine($"Potential refuel detected for asset {assetCode} between {session[startIdx].Rtc} and {session[peakIdx].Rtc}. => {fuelDiff}");

    //                // Accept event
    //                report.AppendLine("<tr>");
    //                report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{assetCode}</td>");
    //                report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{jobCode}</td>");
    //                report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{session[startIdx].Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //                report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{session[peakIdx].Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //                report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{startMedian:F2}</td>");
    //                report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{endMedian:F2}</td>");
    //                report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{fuelDiff:F2}</td>");
    //                report.AppendLine("<td style='border:1px solid #ddd;padding:8px'>Refuel</td>");
    //                report.AppendLine("</tr>");

    //                refuelTotal += fuelDiff;

    //                // jump past the peak to avoid double-counting
    //                i = peakIdx + 1;
    //            }
    //            else
    //            {
    //                // not a valid refuel, continue after start
    //                i = startIdx + 1;
    //            }
    //        }
    //    }

    //    return (report.ToString(), refuelTotal);
    //}

    //// Helper: split records into sessions by time gap
    //static List<List<FuelRecord>> SplitByTimeGap(List<FuelRecord> records, TimeSpan maxGap)
    //{
    //    var sessions = new List<List<FuelRecord>>();
    //    if (records.Count == 0) return sessions;

    //    var current = new List<FuelRecord> { records[0] };
    //    for (int i = 1; i < records.Count; i++)
    //    {
    //        if (records[i].Rtc - records[i - 1].Rtc > maxGap)
    //        {
    //            sessions.Add(current);
    //            current = new List<FuelRecord>();
    //        }
    //        current.Add(records[i]);
    //    }
    //    if (current.Count > 0) sessions.Add(current);
    //    return sessions;
    //}

    //// Median filter to remove spikes
    //static List<double> MedianFilter(List<double> data, int windowSize)
    //{
    //    if (data.Count == 0) return new List<double>();
    //    if (windowSize <= 1) return new List<double>(data);

    //    var result = new List<double>(data.Count);
    //    int half = windowSize / 2;
    //    for (int i = 0; i < data.Count; i++)
    //    {
    //        int start = Math.Max(0, i - half);
    //        int end = Math.Min(data.Count - 1, i + half);
    //        var window = data.GetRange(start, end - start + 1);
    //        result.Add(Median(window));
    //    }
    //    return result;
    //}

    //static double Median(List<double> input)
    //{
    //    if (input == null || input.Count == 0) return 0;
    //    var sorted = input.OrderBy(x => x).ToArray();
    //    int n = sorted.Length;
    //    if (n % 2 == 1) return sorted[n / 2];
    //    return (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
    //}

    //static double ComputeStdDev(List<double> values)
    //{
    //    if (values == null || values.Count == 0) return 0;
    //    double mean = values.Average();
    //    double sumSq = values.Sum(v => (v - mean) * (v - mean));
    //    return Math.Sqrt(sumSq / values.Count);
    //}

    //// Overload to check stability on smoothed numeric series
    //static bool IsStable(List<double> smoothedFuelLevels, int startIdx, int endIdx, double threshold)
    //{
    //    if (startIdx < 0 || endIdx >= smoothedFuelLevels.Count || startIdx > endIdx) return false;
    //    double minFuel = smoothedFuelLevels[startIdx];
    //    double maxFuel = smoothedFuelLevels[startIdx];
    //    for (int i = startIdx; i <= endIdx; i++)
    //    {
    //        minFuel = Math.Min(minFuel, smoothedFuelLevels[i]);
    //        maxFuel = Math.Max(maxFuel, smoothedFuelLevels[i]);
    //    }
    //    return (maxFuel - minFuel) <= threshold;
    //}

    //// Check whether vehicle is stationary for a window (seconds) around index (uses earlier timestamps)
    //static bool IsStationary(List<FuelRecord> records, int centerIndex, int windowSeconds, double speedThreshold)
    //{
    //    if (records == null || records.Count == 0) return false;
    //    DateTime centerTime = records[centerIndex].Rtc;
    //    DateTime windowStart = centerTime.AddSeconds(-windowSeconds);
    //    DateTime windowEnd = centerTime.AddSeconds(windowSeconds);

    //    var window = records.Where(r => r.Rtc >= windowStart && r.Rtc <= windowEnd).ToList();
    //    if (window.Count == 0) return false;
    //    return window.All(r => r.Speed <= speedThreshold);
    //}

    //// Check GPS stability (small variance) in window after index
    //static bool IsGpsStable(List<FuelRecord> records, int centerIndex, int windowSeconds, double varianceThreshold)
    //{
    //    if (records == null || records.Count == 0) return false;
    //    DateTime centerTime = records[centerIndex].Rtc;
    //    DateTime windowStart = centerTime.AddSeconds(-windowSeconds);
    //    DateTime windowEnd = centerTime.AddSeconds(windowSeconds);

    //    var window = records.Where(r => r.Rtc >= windowStart && r.Rtc <= windowEnd).ToList();
    //    if (window.Count == 0) return true; // treat missing GPS as OK, caller already checks multiple things

    //    double latMean = window.Average(r => r.Latitude);
    //    double lonMean = window.Average(r => r.Longitude);
    //    double latVar = window.Sum(r => (r.Latitude - latMean) * (r.Latitude - latMean)) / window.Count;
    //    double lonVar = window.Sum(r => (r.Longitude - lonMean) * (r.Longitude - lonMean)) / window.Count;

    //    return (latVar <= varianceThreshold && lonVar <= varianceThreshold);
    //}

    //// Count number of samples available in seconds-window before index (approx)
    //static int CountSecondsWindow(List<FuelRecord> records, int centerIndex, int windowSeconds)
    //{
    //    DateTime center = records[centerIndex].Rtc;
    //    DateTime start = center.AddSeconds(-windowSeconds);
    //    return records.Count(r => r.Rtc >= start && r.Rtc <= center);
    //}


    //static (string report, double totalRefueled) AnalyzeFuelData(List<FuelRecord> records, string jobCode, string assetCode)
    //{
    //    StringBuilder report = new();
    //    double refuelTotal = 0;

    //    // Thresholds and parameters
    //    const double minRefuelThreshold = 15.0;  // Minimum fuel difference to consider as refuel
    //    const double maxRefuelThreshold = 500.0; // Maximum fuel difference to consider as refuel
    //    const double stationarySpeedThreshold = 2.0; // Speed threshold for stationary detection
    //    const double processNoise = 0.1; // Process noise for Kalman filter
    //    const double measurementNoise = 2.0; // Measurement noise for Kalman filter

    //    if (records == null || records.Count == 0)
    //        return (string.Empty, refuelTotal);

    //    // Initialize the Kalman filter
    //    KalmanFilter kalmanFilter = new KalmanFilter(records[0].FuelLevel, 1.0, processNoise, measurementNoise);

    //    double previousFilteredFuelLevel = records[0].FuelLevel;

    //    for (int i = 1; i < records.Count; i++)
    //    {
    //        // Skip if the vehicle is moving
    //        if (records[i].Speed > stationarySpeedThreshold)
    //            continue;

    //        // Apply the Kalman filter to smooth the fuel level
    //        double filteredFuelLevel = kalmanFilter.Update(records[i].FuelLevel);

    //        // Detect significant increases in the filtered fuel level
    //        double fuelIncrease = filteredFuelLevel - previousFilteredFuelLevel;
    //        if (fuelIncrease >= minRefuelThreshold && fuelIncrease <= maxRefuelThreshold)
    //        {
    //            Console.WriteLine($"Refuel Found for  {assetCode} fuel difference {fuelIncrease}");
    //            report.AppendLine("<tr>");
    //            report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{assetCode}</td>");
    //            report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{jobCode}</td>");
    //            report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{records[i - 1].Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //            report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{records[i].Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //            report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{previousFilteredFuelLevel:F2}</td>");
    //            report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{filteredFuelLevel:F2}</td>");
    //            report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{fuelIncrease:F2}</td>");
    //            report.AppendLine("<td style='border:1px solid #ddd;padding:8px'>Refuel</td>");
    //            report.AppendLine("</tr>");

    //            refuelTotal += fuelIncrease;
    //        }

    //        previousFilteredFuelLevel = filteredFuelLevel;
    //    }

    //    return (report.ToString(), refuelTotal);
    //}


    //static (string report, double totalRefueled) AnalyzeFuelData(List<FuelRecord> records, string jobCode, string assetCode)
    //{
    //    StringBuilder report = new();
    //    double refuelTotal = 0;

    //    const double minRefuelThreshold = 5.0;  // Minimum fuel difference to consider as refuel
    //    const double maxRefuelThreshold = 1000.0; // Maximum fuel difference to consider as refuel
    //    const double stationarySpeedThreshold = 1.5; // Speed threshold for stationary detection
    //    const double undulationThreshold = 2.0; // Undulation threshold for stability detection
    //    const int stabilityWindow = 6; // Stability window for robust checks
    //    const int minRefuelDurationSeconds = 90; // Minimum duration for a refuel event
    //    const double smoothingFactor = 0.5; // Smoothing factor for exponential smoothing

    //    if (records == null || records.Count == 0)
    //        return (string.Empty, refuelTotal);

    //    // Apply exponential smoothing to the fuel levels
    //    List<double> smoothedFuelLevels = ApplyExponentialSmoothing(records.Select(r => r.FuelLevel).ToList(), smoothingFactor);

    //    for (int i = 0; i < records.Count - 1; i++)
    //    {
    //        if (records[i].Speed > stationarySpeedThreshold || smoothedFuelLevels[i] == 0)
    //            continue;

    //        int startIdx = i;
    //        while (i < records.Count - 1 && smoothedFuelLevels[i + 1] > smoothedFuelLevels[i])
    //            i++;

    //        int peakIdx = i;
    //        double startFuel = smoothedFuelLevels[startIdx];
    //        double peakFuel = smoothedFuelLevels[peakIdx];
    //        double fuelDiff = peakFuel - startFuel;

    //        // Check if the fuel difference is within valid thresholds
    //        if (fuelDiff < minRefuelThreshold || fuelDiff > maxRefuelThreshold)
    //            continue;

    //        // Check if the duration of the refuel event is sufficient
    //        TimeSpan refuelDuration = records[peakIdx].Rtc - records[startIdx].Rtc;
    //        if (refuelDuration.TotalSeconds < minRefuelDurationSeconds)
    //            continue;

    //        // Check stability before and after the refuel event
    //        bool isStableBefore = startIdx >= stabilityWindow && IsStable(smoothedFuelLevels, startIdx - stabilityWindow, startIdx, undulationThreshold);
    //        bool isStableAfter = peakIdx + stabilityWindow < records.Count && IsStable(smoothedFuelLevels, peakIdx, peakIdx + stabilityWindow, undulationThreshold);

    //        // Log missing data or anomalies
    //        if (!isStableBefore || !isStableAfter)
    //        {
    //            Console.WriteLine($"Potential anomaly detected for asset {assetCode} between {records[startIdx].Rtc} and {records[peakIdx].Rtc}.");
    //            continue;
    //        }

    //        report.AppendLine("<tr>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{assetCode}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{jobCode}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{records[startIdx].Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{records[peakIdx].Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{startFuel:F2}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{peakFuel:F2}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{fuelDiff:F2}</td>");
    //        report.AppendLine("<td style='border:1px solid #ddd;padding:8px'>Refuel</td>");
    //        report.AppendLine("</tr>");

    //        refuelTotal += fuelDiff;
    //    }

    //    return (report.ToString(), refuelTotal);
    //}

    //static bool IsStable(List<FuelRecord> records, int startIdx, int endIdx, double threshold)
    //{
    //    double minFuel = records[startIdx].FuelLevel;
    //    double maxFuel = records[startIdx].FuelLevel;

    //    for (int i = startIdx; i <= endIdx; i++)
    //    {
    //        minFuel = Math.Min(minFuel, records[i].FuelLevel);
    //        maxFuel = Math.Max(maxFuel, records[i].FuelLevel);
    //    }

    //    return (maxFuel - minFuel) <= threshold;
    //}

    //https://vehicle-tracking-platform-api.nxt-insights.com/platformservicefuel/api/CommonHierarchy/LNT/GetJobDetailsByLoggedinUser?userEmail=tamilselvan.a@ltimindtree.com
    //eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJzcyIsImVtYWlsIjoidGVzdEBsdGltLmNvbSIsImp0aSI6IjZjM2E4NTRhLTQzYjYtNDc4Ny1iZDBkLTRjMTIwN2I4NTg3NCIsImV4cCI6MTc1Njg4OTU4NywiaXNzIjoiYWlzLmRpZ2l0YWwubG50ZWNjLmNvbSIsImF1ZCI6ImFpcy5kaWdpdGFsLmxudGVjYy5jb20ifQ.BwcP7PTtWrvYDp7s4otUizqBLo-6kgSpD3RxlgpUNA4
    // Implement SendEmailReportAsync using L&T Safety Application Email API
    static async Task SendEmailReportAsync(string subject, string emailBody, List<string> recipients, List<string>? ccEmails)
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
                    EmailAttachments = new object[0],
                    EmailSubject = subject,
                    EMailBody = emailBody,
                    Application = "AIS",
                    ModuleName = "CVT-Fuel",
                    Priority = 2
                };

                var jsonPayload = JsonSerializer.Serialize(payload);
                var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");

                var response = await httpClient.PostAsync(apiEndpoint, content);

                if (response.IsSuccessStatusCode)
                {
                    var responseContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"✅ Email sent to {recipient}. Response: {responseContent}");
                }
                else
                {
                    Console.WriteLine($"❌ Failed to send email to {recipient}. Status Code: {response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️ Error sending email to {recipient}: {ex.Message}");
            }
        });

        await Task.WhenAll(tasks);
    }

    static async Task<List<string>> GetJobCodesAsync(string apiUrl, string bearerToken)
    {



        List<string> jobCodes = new();

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", bearerToken);

        try
        {
            var response = await httpClient.GetAsync(apiUrl);
            if (response.IsSuccessStatusCode)
            {
                var jsonResponse = await response.Content.ReadAsStringAsync();
                var jobCodeData = JsonSerializer.Deserialize<Dictionary<string, List<string>>>(jsonResponse);

                if (jobCodeData != null && jobCodeData.ContainsKey("jobCodes"))
                {
                    jobCodes = jobCodeData["jobCodes"];
                }
                else
                {
                    Console.WriteLine("No job codes found in the response.");
                }
            }
            else
            {
                Console.WriteLine($"Failed to fetch job codes. Status Code: {response.StatusCode}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error fetching job codes: {ex.Message}");
        }

        return jobCodes;
    }





    static List<string> LoadAssetCodesFromDatabase(string connString)
    {
        var assetCodes = new List<string>();

        try
        {
            using var conn = new NpgsqlConnection(connString);
            conn.Open();
            using var cmd = new NpgsqlCommand("SELECT distinct dl.asset_code FROM \"Fuel_Prod\".d_lntassetmaster dl", conn);
            using var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                assetCodes.Add(reader.GetString(0));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error fetching asset codes from database: {ex.Message}");
        }

        return assetCodes;
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

        var (report, totalRefueled) = AnalyzeFuelData(records, jobCode, assetCode, endTime);
        Console.WriteLine(report);
        Console.WriteLine($"Total Refueled: {totalRefueled:F2} L");
    }

    static async Task TriggerAllEmails(AppSettings appSettings)
    {
        if (appSettings.ConnectionStrings?.CitusDb == null)
        {
            Console.WriteLine("Database connection string is missing.");
            return;
        }

        if (!DateTime.TryParseExact(appSettings.TestData?.StartTime, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime startTime) ||
            !DateTime.TryParseExact(appSettings.TestData?.EndTime, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime endTime))
        {
            Console.WriteLine("Invalid start or end time format in appsettings.");
            return;
        }

        var assetCodes = LoadAssetCodesFromDatabase(appSettings.ConnectionStrings.CitusDb);
        if (assetCodes.Count == 0)
        {
            Console.WriteLine("No asset codes found in the database.");
            return;
        }

        StringBuilder allReports = new();
        allReports.AppendLine("<h2>Fuel Report</h2>");
        allReports.AppendLine("<h3 style='text-decoration:underline'>Below Are All the Asset Refueling Events:</h3>");
        allReports.AppendLine("<table style='border: 1px solid #ddd; border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;'>");
        allReports.AppendLine("<thead style='background-color: #f4f4f4;'><tr>");
        allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>ASSET_CODE</th>");
        allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>JOBCODE</th>");
        allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>START_TIME</th>");
        allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>END_TIME</th>");
        allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>START_VALUE</th>");
        allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>END_VALUE</th>");
        allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>Refuel Quantity</th>");
        allReports.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>TYPE</th>");
        allReports.AppendLine("</tr></thead>");
        allReports.AppendLine("<tbody>");

        foreach (var assetCode in assetCodes)
        {
            if (!TryGetDeviceId(appSettings.ConnectionStrings.CitusDb, assetCode, out int deviceId, out string? jobCode))
                continue;

            var records = FetchFuelRecords(appSettings.ConnectionStrings.CitusDb, deviceId, startTime, endTime);
            var (report, totalRefueled) = AnalyzeFuelData(records, jobCode ?? "Unknown Job Code", assetCode, endTime);

            if (totalRefueled > 0)
            {
                allReports.AppendLine(report);
            }
        }

        allReports.AppendLine("</tbody></table>");

        if (appSettings.Email != null && !string.IsNullOrEmpty(appSettings.Email.Sender) && !string.IsNullOrEmpty(appSettings.Email.AppPassword) && appSettings.Email.Recipients != null && appSettings.Email.Recipients.Count > 0)
        {
            await SendEmailReportAsync("Fuel Report", allReports.ToString(), appSettings.Email.Recipients, appSettings.Email.CCEmails);
        }
        else
        {
            Console.WriteLine("Email settings are incomplete or no recipients specified.");
        }
    }

    static async Task TriggerSingleEmailWithApiAssetCodes(AppSettings appSettings, string recipientEmail, string senderEmail, string senderPassword)
    {
        if (appSettings.ConnectionStrings?.CitusDb == null)
        {
            Console.WriteLine("Database connection string is missing.");
            return;
        }

        if (string.IsNullOrEmpty(appSettings.TestData?.ApiUrlassetCode) || string.IsNullOrEmpty(appSettings.TestData?.ApiUrljobcode) || string.IsNullOrEmpty(appSettings.TestData?.BearerToken))
        {
            Console.WriteLine("API URLs or Bearer Token are missing in appsettings.");
            return;
        }

        // Fetch asset codes from API
        var assetCodes = await GetAssetCodesFromApi(appSettings.TestData.ApiUrlassetCode, appSettings.TestData.BearerToken);
        if (assetCodes.Count == 0)
        {
            Console.WriteLine("No asset codes found from the API.");
            return;
        }

        // Fetch job codes from API
        var jobCodes = await GetJobCodesFromApi(appSettings.TestData.ApiUrljobcode, appSettings.TestData.BearerToken);
        if (jobCodes.Count == 0)
        {
            Console.WriteLine("No job codes found from the API.");
            return;
        }

        Console.WriteLine("Enter start time (yyyy-MM-dd HH:mm:ss):");
        string? startTimeInput = Console.ReadLine();
        Console.WriteLine("Enter end time (yyyy-MM-dd HH:mm:ss):");
        string? endTimeInput = Console.ReadLine();

        if (!DateTime.TryParseExact(startTimeInput, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime startTime) ||
            !DateTime.TryParseExact(endTimeInput, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out DateTime endTime))
        {
            Console.WriteLine("Invalid start or end time format.");
            return;
        }

        StringBuilder emailBody = new();
        emailBody.AppendLine("<html><body>");
        emailBody.AppendLine("<h2>Fuel Report</h2>");
        emailBody.AppendLine("<h3 style='text-decoration:underline'>Below Are All the Asset Refueling Events:</h3>");
        emailBody.AppendLine("<table style='border: 1px solid #ddd; border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;'>");
        emailBody.AppendLine("<thead style='background-color: #f4f4f4;'><tr>");
        emailBody.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>ASSET_CODE</th>");
        emailBody.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>JOBCODE</th>");
        emailBody.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>START_TIME</th>");
        emailBody.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>END_TIME</th>");
        emailBody.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>START_VALUE</th>");
        emailBody.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>END_VALUE</th>");
        emailBody.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>AMOUNT</th>");
        emailBody.AppendLine("<th style='border: 1px solid #ddd; padding: 8px;'>TYPE</th>");
        emailBody.AppendLine("</tr></thead>");
        emailBody.AppendLine("<tbody>");

        foreach (var assetCode in assetCodes)
        {
            if (!TryGetDeviceId(appSettings.ConnectionStrings.CitusDb, assetCode, out int deviceId, out string? jobCode))
                continue;

            var records = FetchFuelRecords(appSettings.ConnectionStrings.CitusDb, deviceId, startTime, endTime);
            var (report, totalRefueled) = AnalyzeFuelData(records, jobCode ?? "Unknown Job Code", assetCode, endTime);

            if (totalRefueled > 0)
            {
                emailBody.AppendLine(report);
            }
        }

        emailBody.AppendLine("</tbody></table>");
        emailBody.AppendLine("</body></html>");

        // Send email using the provided sender credentials
        await SendEmailReportAsync("Fuel Report", emailBody.ToString(), new List<string> { recipientEmail }, null);
    }

    // Define the GetAssetCodesFromApi method
    static async Task<List<string>> GetAssetCodesFromApi(string apiUrl, string bearerToken)
    {
        List<string> assetCodes = new();

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", bearerToken);

        try
        {
            var response = await httpClient.GetAsync(apiUrl);
            if (response.IsSuccessStatusCode)
            {
                var jsonResponse = await response.Content.ReadAsStringAsync();
                var assetCodeData = JsonSerializer.Deserialize<Dictionary<string, List<string>>>(jsonResponse);

                if (assetCodeData != null && assetCodeData.ContainsKey("assetCodes"))
                {
                    assetCodes = assetCodeData["assetCodes"];
                }
                else
                {
                    Console.WriteLine("No asset codes found in the response.");
                }
            }
            else
            {
                Console.WriteLine($"Failed to fetch asset codes. Status Code: {response.StatusCode}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error fetching asset codes: {ex.Message}");
        }

        return assetCodes;
    }

    // Define the GetJobCodesFromApi method
    static async Task<List<string>> GetJobCodesFromApi(string apiUrl, string bearerToken)
    {
        List<string> jobCodes = new();
        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", bearerToken);
        try
        {
            var response = await httpClient.GetAsync(apiUrl);
            if (response.IsSuccessStatusCode)
            {
                var jsonResponse = await response.Content.ReadAsStringAsync();

                using var doc = System.Text.Json.JsonDocument.Parse(jsonResponse);
                if (doc.RootElement.TryGetProperty("result", out var resultProp))
                {
                    foreach (var job in resultProp.EnumerateArray())
                    {
                        if (job.TryGetProperty("job_Code", out var jobCodeProp))
                        {
                            var jobCode = jobCodeProp.GetString();
                            if (!string.IsNullOrEmpty(jobCode)) jobCodes.Add(jobCode);
                        }
                    }
                }
                else
                {
                    Console.WriteLine("No 'result' property found in job code API response.");
                }
            }
            else
            {
                Console.WriteLine($"Failed to fetch job codes. Status Code: {response.StatusCode}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error fetching job codes: {ex.Message}");
        }

        return jobCodes;
    }




    static async Task<string?> GetTokenFromLogin(string apiUrl, AppSettings appSettings)
    {
        using var httpClient = new HttpClient();
          
        // Hardcoded credentials from appsettings.json
        var credentials = new
        {
            UserEmailId = appSettings.loginCredentials?.Username,
            Password = appSettings.loginCredentials?.Password
        };

        var jsonPayload = JsonSerializer.Serialize(credentials);
        var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");

        try
        {
            Console.WriteLine("Payload being sent:");
            Console.WriteLine(jsonPayload); // Log the payload for debugging

            var response = await httpClient.PostAsync(apiUrl, content);
            if (response.IsSuccessStatusCode)
            {
                var jsonResponse = await response.Content.ReadAsStringAsync();
                Console.WriteLine("Raw JSON Response:");
                Console.WriteLine(jsonResponse); // Log the raw JSON response for debugging

                var tokenData = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonResponse);

                if (tokenData != null && tokenData.ContainsKey("data"))
                {
                    var data = JsonSerializer.Deserialize<Dictionary<string, object>>(tokenData["data"].ToString()!);
                    if (data != null && data.ContainsKey("Token"))
                    {
                        return data["Token"]?.ToString();
                    }
                }

                Console.WriteLine("Token not found in the response.");
            }
            else
            {
                Console.WriteLine($"Failed to fetch token. Status Code: {response.StatusCode}");
                var errorResponse = await response.Content.ReadAsStringAsync();
                Console.WriteLine("Error Response:");
                Console.WriteLine(errorResponse); // Log the error response for debugging
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error fetching token: {ex.Message}");
        }

        return null;
    }

    private static List<string> GetAssetCodesForSbgAndIc(string connectionString)
    {
        var assetCodes = new List<string>();

        using (var conn = new NpgsqlConnection(connectionString))
        {
            conn.Open();

            var sql = @"
            SELECT DISTINCT am.asset_code
            FROM ""Fuel_Prod"".d_lntassetmaster am
            JOIN ""Fuel_Prod"".""VW_JOBMASTER"" vj ON am.jobcode = vj.job_code
            WHERE vj.sbg_name IN (
                'Default SBG',
                'Irrigation, Industrial & Infrastructure SBG',
                'Larsen & Toubro Limited – PCIPL JV SBG'
            )
            AND vj.ic_name IN (
                'Water & Effluent Treatment IC',
                'Larsen & Toubro Limited – PCIPL JV'
            )";

            using var cmd = new NpgsqlCommand(sql, conn);
            using var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                var assetCode = reader.GetString(0);
                assetCodes.Add(assetCode);
            }
        }

        Console.WriteLine($"[Mode 4] Total asset codes found for SBG+IC filter: {assetCodes.Count}");
        return assetCodes;
    }

    private static List<string> GetAssetCodesForSpecificIC(string connectionString)
    {

        var assetCodes = new List<string>();

        using (var conn = new NpgsqlConnection(connectionString))
        {
            conn.Open();

            var sql = @"
            SELECT DISTINCT am.asset_code 
            FROM ""Fuel_Prod"".d_lntassetmaster am
            JOIN ""Fuel_Prod"".d_eipjobmaster dejm ON am.jobcode = dejm.job_code
            WHERE dejm.ic_name IN ('Water & Effluent Treatment IC', 'Larsen & Toubro Limited – PCIPL JV')";

            using var cmd = new NpgsqlCommand(sql, conn);
            using var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                var assetCode = reader.GetString(0);
                assetCodes.Add(assetCode);
                Console.WriteLine($"Found asset code: {assetCode}"); // Added logging
            }
        }

        Console.WriteLine($"Total asset codes found: {assetCodes.Count}");
        return assetCodes;
    }


    //static (string report, double totalRefueled) AnalyzeFuelData(
    // List<FuelRecord> records, string jobCode, string assetCode)
    //{
    //    StringBuilder report = new();
    //    double refuelTotal = 0;

    //    // ── Tunable thresholds ────────────────────────────────────────────────────
    //    const double minRefuelThreshold = 10.0;   // min net fuel gain (L)
    //    const double maxRefuelThreshold = 1200.0; // sanity cap (L)
    //    const double maxSpeedDuringRefuel = 0.0;    // km/h — "stationary" threshold
    //    const int speedSpikeTolerance = 2;      // consecutive fast records before "moving"
    //    const double sensorNoiseTolerance = 4.0;    // per-step dip allowed mid-fill (L)
    //    const int noiseLookahead = 3;      // consecutive noisy steps before aborting rise
    //    const int minRefuelDurationSec = 50;     // minimum wall-clock fill time (s)
    //    const int stabilityWindowSec = 100;     // pre/post stability check window (seconds)
    //    const double stabilityBand = 5.0;    // allowed fuel swing in stable window (L)

    //    // NEW: minimum accepted gain when ONLY one side is stable.
    //    // User specified "20 to 30 litres" — set to 20.0 (tunable).
    //    const double oneSideMinLiters = 20.0;
    //    const double oneSideMaxLitres = 30.0;
    //    const int oneSidehighduration = 300;
    //    // ──────────────────────────────────────────────────────────────────────────

    //    if (records == null || records.Count < 2)
    //        return (string.Empty, refuelTotal);

    //    var recs = records
    //        .Where(r => r.FuelLevel > 0 && r.Rtc != DateTime.MinValue)
    //        .OrderBy(r => r.Rtc)
    //        .ToList();

    //    if (recs.Count < 2)
    //        return (string.Empty, refuelTotal);

    //    int n = recs.Count;
    //    int i = 0;

    //    while (i < n - 1)
    //    {
    //        if (IsSustainedMovement(recs, i, maxSpeedDuringRefuel, speedSpikeTolerance))
    //        {
    //            i++;
    //            continue;
    //        }

    //        int troughIdx = FindTrough(recs, i, maxSpeedDuringRefuel, sensorNoiseTolerance);

    //        int peakIdx = FindPeak(recs, troughIdx,
    //                               maxSpeedDuringRefuel, speedSpikeTolerance,
    //                               sensorNoiseTolerance, noiseLookahead,
    //                               out int nextScanIdx);

    //        i = Math.Max(troughIdx + 1, nextScanIdx);

    //        double startFuel = recs[troughIdx].FuelLevel;
    //        double peakFuel = recs[peakIdx].FuelLevel;
    //        double fuelDiff = peakFuel - startFuel;

    //        if (fuelDiff < minRefuelThreshold || fuelDiff > maxRefuelThreshold)
    //            continue;

    //        TimeSpan duration = recs[peakIdx].Rtc - recs[troughIdx].Rtc;
    //        if (duration.TotalSeconds < minRefuelDurationSec)
    //            continue;

    //        bool stableBefore = IsStableBeforeTime(recs, troughIdx, stabilityWindowSec, stabilityBand);
    //        bool stableAfter = IsStableAfterTime(recs, peakIdx, stabilityWindowSec, stabilityBand, n);

    //        // NEW: Apply acceptance rules per user request:
    //        // 1) Both stable => accept (High confidence)
    //        // 2) One side stable only => accept only if fuelDiff >= oneSideMinLiters (Low confidence)
    //        // 3) Neither stable => reject
    //        string? confidence = null;
    //        if (stableBefore && stableAfter && duration.TotalSeconds>=stabilityWindowSec)
    //        {
    //            confidence = "High";
    //        }

    //        else if (stableBefore ^ stableAfter) // exactly one is true
    //        {
    //            if (fuelDiff >= oneSideMaxLitres || duration.TotalSeconds >= oneSidehighduration)
    //            {
    //                confidence = "High";

    //            }
    //            else if(fuelDiff>=oneSideMinLiters)
    //            {
    //                confidence = "Low";
    //            }

    //            else
    //            {
    //                continue;
    //            }

    //        }
    //        else
    //        {
    //            // Too small a gain when only one side is stable — skip
    //            continue;
    //        }



    //        Console.WriteLine(
    //            $"[{confidence.ToUpper()}] Asset={assetCode} | " +
    //            $"{recs[troughIdx].Rtc:yyyy-MM-dd HH:mm:ss} → {recs[peakIdx].Rtc:yyyy-MM-dd HH:mm:ss} | " +
    //            $"Gain={fuelDiff:F2}L | Dur={duration.TotalSeconds:F0}s | " +
    //            $"StableBefore={stableBefore} StableAfter={stableAfter}");

    //        AppendRefuelRow(report, assetCode, jobCode,
    //                        recs[troughIdx], recs[peakIdx],
    //                        startFuel, peakFuel, fuelDiff, confidence);

    //        refuelTotal += fuelDiff;
    //    }

    //    return (report.ToString(), refuelTotal);
    //}

    //static bool IsSustainedMovement(
    //List<FuelRecord> recs, int idx,
    //double maxSpeed, int toleranceCount)
    //{
    //    if (recs[idx].Speed <= maxSpeed) return false;

    //    int fast = 1;
    //    for (int k = idx + 1;
    //         k < recs.Count && k <= idx + toleranceCount;
    //         k++)
    //    {
    //        if (recs[k].Speed > maxSpeed) fast++;
    //    }
    //    return fast >= toleranceCount;
    //}


    //static int FindTrough(
    //List<FuelRecord> recs, int startIdx,
    //double maxSpeed, double noiseTolerance)
    //{
    //    int trough = startIdx;
    //    double troughFuel = recs[startIdx].FuelLevel;

    //    for (int k = startIdx + 1; k < recs.Count; k++)
    //    {
    //        if (recs[k].Speed > maxSpeed) break;

    //        double delta = recs[k].FuelLevel - recs[k - 1].FuelLevel;

    //        // A clear upward step means the trough is behind us — stop.
    //        if (delta > noiseTolerance) break;

    //        // Still falling or flat — update trough.
    //        if (recs[k].FuelLevel <= troughFuel)
    //        {
    //            troughFuel = recs[k].FuelLevel;
    //            trough = k;
    //        }
    //    }
    //    return trough;
    //}

    //static int FindPeak(
    //List<FuelRecord> recs, int troughIdx,
    //double maxSpeed, int speedTolerance,
    //double noiseTolerance, int noiseLookahead,
    //out int nextScanIdx)
    //{
    //    int peakIdx = troughIdx;
    //    double peakFuel = recs[troughIdx].FuelLevel;
    //    int noisy = 0;
    //    int j = troughIdx + 1;

    //    while (j < recs.Count)
    //    {
    //        // Sustained movement ends the fill.
    //        if (IsSustainedMovement(recs, j, maxSpeed, speedTolerance))
    //            break;

    //        double delta = recs[j].FuelLevel - recs[j - 1].FuelLevel;

    //        if (delta < -noiseTolerance)
    //        {
    //            // Significant drop — consume lookahead before giving up.
    //            if (++noisy > noiseLookahead) break;
    //        }
    //        else
    //        {
    //            // Good step (rise or small dip) — reset noise counter.
    //            noisy = 0;
    //        }

    //        if (recs[j].FuelLevel > peakFuel)
    //        {
    //            peakFuel = recs[j].FuelLevel;
    //            peakIdx = j;
    //        }

    //        j++;
    //    }

    //    nextScanIdx = j;
    //    return peakIdx;
    //}

    //static bool IsStableBeforeTime(
    //List<FuelRecord> recs, int startIdx,
    //int windowSec, double band)
    //{
    //    var cutoff = recs[startIdx].Rtc.AddSeconds(-windowSec);
    //    var vals = recs
    //        .Take(startIdx)
    //        .Where(r => r.Rtc >= cutoff)
    //        .Select(r => r.FuelLevel)
    //        .ToList();

    //    return IsStableValues(vals, band);
    //}

    //static bool IsStableAfterTime(
    //List<FuelRecord> recs, int peakIdx,
    //int windowSec, double band, int n)
    //{
    //    var cutoff = recs[peakIdx].Rtc.AddSeconds(windowSec);
    //    var vals = recs
    //        .Skip(peakIdx)
    //        .TakeWhile(r => r.Rtc <= cutoff)
    //        .Select(r => r.FuelLevel)
    //        .ToList();

    //    return IsStableValues(vals, band);
    //}

    //static bool IsStableValues(List<double> vals, double band)
    //{
    //    if (vals.Count < 2) return false;
    //    return (vals.Max() - vals.Min()) <= band;
    //}

    static bool IsStable(List<FuelRecord> records, int startIdx, int endIdx, double threshold)
    {
        double min = records[startIdx].FuelLevel;
        double max = records[startIdx].FuelLevel;
        for (int i = startIdx + 1; i <= endIdx && i < records.Count; i++)
        {
            if (records[i].FuelLevel < min) min = records[i].FuelLevel;
            if (records[i].FuelLevel > max) max = records[i].FuelLevel;
        }
        return (max - min) <= threshold;
    }

    // --- Helper: emit one HTML table row ---
    //    static void AppendRefuelRow(
    //        StringBuilder report,
    //        string assetCode,
    //        string jobCode,
    //        FuelRecord start,
    //        FuelRecord peak,
    //        double startFuel,
    //        double peakFuel,
    //        double fuelDiff,
    //        string confidence)
    //    {
    //        // Use an amber background for low-confidence rows to visually distinguish them.
    //        string rowStyle = confidence?.Equals("Low", StringComparison.OrdinalIgnoreCase) == true
    //            ? "background-color:#fff3cd"
    //            : "";

    //        report.AppendLine($"<tr style='{rowStyle}'>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{assetCode}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{jobCode}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{start.Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{peak.Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{startFuel:F2}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{peakFuel:F2}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{fuelDiff:F2}</td>");
    //        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>Refuel ({confidence})</td>");
    //        report.AppendLine("</tr>");
    //    }

    //}



    //static (string report, double totalRefueled) AnalyzeFuelData(
    //   List<FuelRecord> records, string jobCode, string assetCode)
    //{
    //    StringBuilder report = new();
    //    double refuelTotal = 0;

    //    // ── Tunable thresholds ────────────────────────────────────────────────────
    //    const double minRefuelThreshold = 10.0;      // min fuel gain to count as refuel (L)
    //    const double maxRefuelThreshold = 1200.0;    // sanity cap (L)
    //    const int timeWindowMinutes = 20;            // time-based window (minutes)
    //    const double maxSpeedDuringRefuel = 0.0;     // vehicle must be stationary
    //    const double stabilityBand = 2.0;            // allowed fuel swing in stable window (L)
    //    const int stabilityWindowSec = 90;           // pre/post stability check window (seconds)
    //    const double minFuelChangeToBreakEvent = -0.5;  // significant decrease to break event
    //    // ──────────────────────────────────────────────────────────────────────────

    //    if (records == null || records.Count < 2)
    //        return (string.Empty, refuelTotal);

    //    var recs = records
    //        .Where(r => r.FuelLevel > 0 && r.Rtc != DateTime.MinValue)
    //        .OrderBy(r => r.Rtc)
    //        .ToList();

    //    if (recs.Count < 2)
    //        return (string.Empty, refuelTotal);

    //    int n = recs.Count;
    //    var processedIndices = new HashSet<int>();

    //    int i = 0;
    //    while (i < n)
    //    {
    //        // Skip if already processed
    //        if (processedIndices.Contains(i))
    //        {
    //            i++;
    //            continue;
    //        }

    //        // Skip if vehicle is moving
    //        if (recs[i].Speed > maxSpeedDuringRefuel)
    //        {
    //            i++;
    //            continue;
    //        }

    //        // ═══ PHASE 1: Find the START (minimum fuel point) ═══
    //        int startIdx = i;
    //        double startFuel = recs[i].FuelLevel;
    //        int peakIdx = i;
    //        double peakFuel = recs[i].FuelLevel;

    //        // Walk backwards to find if this is the true minimum in a potential refuel event
    //        int lookbackIdx = i;
    //        while (lookbackIdx > 0)
    //        {
    //            // Check if the gap between current and previous record is reasonable
    //            double timeSec = (recs[lookbackIdx].Rtc - recs[lookbackIdx - 1].Rtc).TotalSeconds;

    //            // Break if time gap is too large (more than 5 minutes)
    //            if (timeSec > 300)
    //                break;

    //            // Break if vehicle was moving at the previous point
    //            if (recs[lookbackIdx - 1].Speed > maxSpeedDuringRefuel)
    //                break;

    //            // If previous fuel level is lower, update start point
    //            if (recs[lookbackIdx - 1].FuelLevel < startFuel)
    //            {
    //                lookbackIdx--;
    //                startIdx = lookbackIdx;
    //                startFuel = recs[lookbackIdx].FuelLevel;
    //            }
    //            else
    //            {
    //                // Fuel is not decreasing, so we've found the trough
    //                break;
    //            }
    //        }
    //        // ═══ PHASE 2: Walk forward to find ENTIRE refuel sequence ═══
    //        // Continue through rises, stable periods, and small dips until we find a significant drop
    //        int j = startIdx + 1;
    //        bool foundRise = false;

    //        while (j < n)
    //        {
    //            // Stop if vehicle moves
    //            if (recs[j].Speed > maxSpeedDuringRefuel)
    //                break;

    //            double fuelChange = recs[j].FuelLevel - recs[j - 1].FuelLevel;

    //            // Track if we've found any rise
    //            if (fuelChange > 0.2)
    //            {
    //                foundRise = true;
    //            }

    //            // Update peak if we find a higher value
    //            if (recs[j].FuelLevel > peakFuel)
    //            {
    //                peakFuel = recs[j].FuelLevel;
    //                peakIdx = j;
    //            }

    //            // CRITICAL: Only break on SIGNIFICANT decrease (not during refuel pauses)
    //            if (foundRise && fuelChange < minFuelChangeToBreakEvent)
    //            {
    //                // Significant drop - this event has ended
    //                break;
    //            }

    //            j++;

    //            // Also break if we exceed the time window
    //            if ((recs[j - 1].Rtc - recs[startIdx].Rtc).TotalMinutes > timeWindowMinutes)
    //            {
    //                break;
    //            }
    //        }

    //        // If no rise was found, skip this point
    //        if (!foundRise || peakIdx == startIdx)
    //        {
    //            i++;
    //            continue;
    //        }

    //        // ═══ PHASE 3: Calculate fuel difference ═══
    //        double fuelDiff = peakFuel - startFuel;

    //        // Gate 1: Check if gain is significant
    //        if (fuelDiff < minRefuelThreshold || fuelDiff > maxRefuelThreshold)
    //        {
    //            i++;
    //            continue;
    //        }

    //        // ═══ PHASE 4: Validate time duration ═══
    //        TimeSpan duration = recs[peakIdx].Rtc - recs[startIdx].Rtc;
    //        if (duration.TotalSeconds < 20)  // At least 30 seconds of refuel
    //        {
    //            i++;
    //            continue;
    //        }

    //        // ═══ PHASE 5: Check stability before and after ═══
    //        bool stableBefore = IsStableBeforeTime(recs, startIdx, stabilityWindowSec, stabilityBand);
    //        bool stableAfter = IsStableAfterTime(recs, peakIdx, stabilityWindowSec, stabilityBand, n);

    //        // ═══ PHASE 6: Determine confidence level ═══
    //        string confidence = "Low";
    //        if (stableBefore && stableAfter)
    //        {
    //            confidence = "High";
    //        }
    //        else if (stableBefore || stableAfter)
    //        {
    //            confidence = "Medium";
    //        }

    //        // ═══ PHASE 7: Record the refuel event ═══
    //        Console.WriteLine(
    //            $"[{confidence.ToUpper()}] Asset={assetCode} | " +
    //            $"{recs[startIdx].Rtc:yyyy-MM-dd HH:mm:ss} → {recs[peakIdx].Rtc:yyyy-MM-dd HH:mm:ss} | " +
    //            $"Start={startFuel:F2}L → Peak={peakFuel:F2}L | Gain={fuelDiff:F2}L | Dur={duration.TotalSeconds:F0}s | " +
    //            $"StableBefore={stableBefore} StableAfter={stableAfter}");

    //        AppendRefuelRow(report, assetCode, jobCode,
    //                        recs[startIdx], recs[peakIdx],
    //                        startFuel, peakFuel, fuelDiff, confidence);

    //        refuelTotal += fuelDiff;

    //        // Mark all indices in this event as processed
    //        for (int k = startIdx; k <= peakIdx && k < n; k++)
    //        {
    //            processedIndices.Add(k);
    //        }

    //        // Move to next unprocessed record after this event
    //        i = peakIdx + 1;
    //    }

    //    return (report.ToString(), refuelTotal);
    //}


    //static (string report, double totalRefueled) AnalyzeFuelData(
    // List<FuelRecord> records, string jobCode, string assetCode)
    //{
    //    // ── Tunable thresholds ────────────────────────────────────────────────────
    //    const double minRefuelThreshold = 10.0;      // min fuel gain to count as refuel (L)
    //    const double maxRefuelThreshold = 1200.0;    // sanity cap (L)
    //    const int timeWindowMinutes = 15;            // time-based window (minutes)
    //    const double maxSpeedDuringRefuel = 0.0;     // vehicle must be stationary
    //    const double stabilityBand = 1.0;            // allowed fuel swing in stable window (L)
    //    const int stabilityWindowSec = 180;           // pre/post stability check window (seconds)
    //    const double minFuelChangeToBreakEvent = -0.5;   // significant decrease to break event (was -0.5)
    //    const double longGapMinutes = 15;             // new: treat gaps longer than this as potential refuels
    //                                                  // ──────────────────────────────────────────────────────────────────────────

    //    // Local builder – the parameter 'string report' is unused and ignored.
    //    var reportBuilder = new StringBuilder();
    //    double refuelTotal = 0;

    //    if (records == null || records.Count < 2)
    //        return (string.Empty, refuelTotal);

    //    var recs = records
    //        .Where(r => r.FuelLevel > 0 && r.Rtc != DateTime.MinValue)
    //        .OrderBy(r => r.Rtc)
    //        .ToList();

    //    if (recs.Count < 2)
    //        return (string.Empty, refuelTotal);

    //    int n = recs.Count;
    //    var processedIndices = new HashSet<int>();

    //    // ═══════════════════════════════════════════════════════════════════════════
    //    // PHASE 1: Gap‑based detection (handles long interruptions)
    //    // ═══════════════════════════════════════════════════════════════════════════
    //    for (int i = 0; i < n - 1; i++)
    //    {
    //        if (processedIndices.Contains(i) || processedIndices.Contains(i + 1))
    //            continue;

    //        double gapMinutes = (recs[i + 1].Rtc - recs[i].Rtc).TotalMinutes;
    //        if (gapMinutes <= longGapMinutes)
    //            continue;

    //        double fuelIncrease = recs[i + 1].FuelLevel - recs[i].FuelLevel;
    //        if (fuelIncrease < minRefuelThreshold || fuelIncrease > maxRefuelThreshold)
    //            continue;

    //        // Optional speed check – if speed data is missing, ensure Speed defaults to 0.
    //        if (recs[i].Speed > maxSpeedDuringRefuel || recs[i + 1].Speed > maxSpeedDuringRefuel)
    //            continue;

    //        bool stableBefore = IsStableBeforeTime(recs, i, stabilityWindowSec, stabilityBand);
    //        bool stableAfter = IsStableAfterTime(recs, i + 1, stabilityWindowSec, stabilityBand, n);
    //        string confidence = (stableBefore && stableAfter) ? "High" : (stableBefore || stableAfter) ? "Medium" : "Low";

    //        AppendRefuelRow(reportBuilder, assetCode, jobCode,
    //                        recs[i], recs[i + 1],
    //                        recs[i].FuelLevel, recs[i + 1].FuelLevel,
    //                        fuelIncrease, confidence + " (gap)");

    //        refuelTotal += fuelIncrease;
    //        processedIndices.Add(i);
    //        processedIndices.Add(i + 1);
    //    }

    //    // ═══════════════════════════════════════════════════════════════════════════
    //    // PHASE 2: Sequential detection (original logic, with updated break threshold)
    //    // ═══════════════════════════════════════════════════════════════════════════
    //    int idx = 0;
    //    while (idx < n)
    //    {
    //        // Skip if already processed
    //        if (processedIndices.Contains(idx))
    //        {
    //            idx++;
    //            continue;
    //        }

    //        // Skip if vehicle is moving
    //        if (recs[idx].Speed > maxSpeedDuringRefuel)
    //        {
    //            idx++;
    //            continue;
    //        }

    //        // Find the START (minimum fuel point)
    //        int startIdx = idx;
    //        double startFuel = recs[idx].FuelLevel;
    //        int peakIdx = idx;
    //        double peakFuel = recs[idx].FuelLevel;

    //        // Walk backwards to find true minimum in potential refuel event
    //        int lookbackIdx = idx;
    //        while (lookbackIdx > 0)
    //        {
    //            double timeSec = (recs[lookbackIdx].Rtc - recs[lookbackIdx - 1].Rtc).TotalSeconds;
    //            if (timeSec > 300) break;                       // gap too large
    //            if (recs[lookbackIdx - 1].Speed > maxSpeedDuringRefuel) break; // moving

    //            if (recs[lookbackIdx - 1].FuelLevel < startFuel)
    //            {
    //                lookbackIdx--;
    //                startIdx = lookbackIdx;
    //                startFuel = recs[lookbackIdx].FuelLevel;
    //            }
    //            else
    //            {
    //                break; // found trough
    //            }
    //        }

    //        // Walk forward to find the refuel sequence
    //        int j = startIdx + 1;
    //        bool foundRise = false;

    //        while (j < n)
    //        {
    //            if (recs[j].Speed > maxSpeedDuringRefuel)
    //                break;

    //            double fuelChange = recs[j].FuelLevel - recs[j - 1].FuelLevel;

    //            if (fuelChange > 0.2)
    //                foundRise = true;

    //            if (recs[j].FuelLevel > peakFuel)
    //            {
    //                peakFuel = recs[j].FuelLevel;
    //                peakIdx = j;
    //            }

    //            // Break only on significant decrease (updated threshold)
    //            if (foundRise && fuelChange < minFuelChangeToBreakEvent)
    //                break;

    //            j++;

    //            if ((recs[j - 1].Rtc - recs[startIdx].Rtc).TotalMinutes > timeWindowMinutes)
    //                break;
    //        }

    //        if (!foundRise || peakIdx == startIdx)
    //        {
    //            idx++;
    //            continue;
    //        }

    //        double fuelDiff = peakFuel - startFuel;
    //        if (fuelDiff < minRefuelThreshold || fuelDiff > maxRefuelThreshold)
    //        {
    //            idx++;
    //            continue;
    //        }

    //        TimeSpan duration = recs[peakIdx].Rtc - recs[startIdx].Rtc;
    //        if (duration.TotalSeconds < 30)   // at least 20 seconds of refuel
    //        {
    //            idx++;
    //            continue;
    //        }

    //        bool stableBefore = IsStableBeforeTime(recs, startIdx, stabilityWindowSec, stabilityBand);
    //        bool stableAfter = IsStableAfterTime(recs, peakIdx, stabilityWindowSec, stabilityBand, n);

    //        string confidence = "Low";
    //        if (stableBefore && stableAfter)
    //            confidence = "High";
    //        else if (stableBefore || stableAfter)
    //            confidence = "Medium";

    //        // Log to console (optional)
    //        Console.WriteLine(
    //            $"[{confidence.ToUpper()}] Asset={assetCode} | " +
    //            $"{recs[startIdx].Rtc:yyyy-MM-dd HH:mm:ss} → {recs[peakIdx].Rtc:yyyy-MM-dd HH:mm:ss} | " +
    //            $"Start={startFuel:F2}L → Peak={peakFuel:F2}L | Gain={fuelDiff:F2}L | Dur={duration.TotalSeconds:F0}s | " +
    //            $"StableBefore={stableBefore} StableAfter={stableAfter}");

    //        AppendRefuelRow(reportBuilder, assetCode, jobCode,
    //                        recs[startIdx], recs[peakIdx],
    //                        startFuel, peakFuel, fuelDiff, confidence);

    //        refuelTotal += fuelDiff;

    //        // Mark all indices in this event as processed
    //        for (int k = startIdx; k <= peakIdx && k < n; k++)
    //            processedIndices.Add(k);

    //        idx = peakIdx + 1;
    //    }

    //    return (reportBuilder.ToString(), refuelTotal);
    //}



    static bool IsUndulation(List<FuelRecord> recs, int startIdx,
        int maxSmallDiffRecords = 5,
        double smallDiffThreshold = 1.0)
    {
        // Safety check: ensure we have enough records ahead
        if (startIdx + 10 >= recs.Count)
            return false; // Not enough data ahead, treat as potential refuel

        double minFuel = recs[startIdx].FuelLevel;
        var fuelDifferences = new List<double>();
        var recordIndices = new List<int>();
        int smallDiffCount = 0;
        double maxDifferenceFound = 0;
        int peakIndex = startIdx;
        double peakFuel = minFuel;

        // Analyze all subsequent records
        for (int i = startIdx + 1; i < recs.Count && i < startIdx + 20; i++)
        {
            double currentFuel = recs[i].FuelLevel;
            double difference = currentFuel - minFuel;

            fuelDifferences.Add(difference);
            recordIndices.Add(i);

            // Track peak
            if (currentFuel > peakFuel)
            {
                peakFuel = currentFuel;
                peakIndex = i;
            }

            // Count records with small differences
            if (difference <= smallDiffThreshold)
            {
                smallDiffCount++;
            }

            // Track max difference found
            if (difference > maxDifferenceFound)
            {
                maxDifferenceFound = difference;
            }

            // Early exit: if peak is found far from min with sustained growth, it's real refuel
            if (difference > 5.0)
            {
                Console.WriteLine(
                    $"[REAL REFUEL - PEAK DETECTED] StartIdx={startIdx} MinFuel={minFuel:F2}L | " +
                    $"At Record {i}: Peak={currentFuel:F2}L (Diff={difference:F2}L) | " +
                    $"Small diffs count: {smallDiffCount}/{i - startIdx} → ACCEPTING (Significant Peak)");
                return false; // Real refuel with clear peak
            }
        }

        // Calculate statistics
        double avgDifference = fuelDifferences.Count > 0 ? fuelDifferences.Average() : 0;
        double variance = fuelDifferences.Count > 0
            ? fuelDifferences.Sum(d => (d - avgDifference) * (d - avgDifference)) / fuelDifferences.Count
            : 0;
        double stdDev = Math.Sqrt(variance);

        // Calculate percentage of records with small differences
        double smallDiffPercentage = fuelDifferences.Count > 0
            ? (smallDiffCount * 100.0) / fuelDifferences.Count
            : 0;

        // UNDULATION DETECTION LOGIC:
        // If MORE than 5 records show ≤ 1L difference → likely oscillation/sensor noise
        bool isUndulation = smallDiffCount > maxSmallDiffRecords;

        if (isUndulation)
        {
            Console.WriteLine(
                $"[UNDULATION DETECTED] StartIdx={startIdx} MinFuel={minFuel:F2}L | " +
                $"Analyzed {fuelDifferences.Count} records: {smallDiffCount} records with diff ≤ {smallDiffThreshold}L (>{maxSmallDiffRecords}) | " +
                $"Diffs={string.Join(",", fuelDifferences.Take(10).Select(d => d.ToString("F2")))}{(fuelDifferences.Count > 10 ? "..." : "")} | " +
                $"Max={maxDifferenceFound:F2}L Avg={avgDifference:F2}L StdDev={stdDev:F3} SmallDiff%={smallDiffPercentage:F1}% → SKIPPING (Oscillation/Sensor Noise)");
        }
        else if (maxDifferenceFound >= smallDiffThreshold && peakIndex > startIdx)
        {
            double peakGain = peakFuel - minFuel;
            bool isGradualRise = true;

            // Verify gradual rise pattern (no significant drops after peak)
            for (int i = peakIndex + 1; i < Math.Min(peakIndex + 5, recs.Count); i++)
            {
                if (recs[i].FuelLevel < recs[i - 1].FuelLevel - 0.5)
                {
                    isGradualRise = false;
                    break;
                }
            }

            if (isGradualRise)
            {
                Console.WriteLine(
                    $"[GRADUAL RISE DETECTED] StartIdx={startIdx} MinFuel={minFuel:F2}L | " +
                    $"Peak at Record {peakIndex}: {peakFuel:F2}L (Gain={peakGain:F2}L) | " +
                    $"Small diffs: {smallDiffCount}/{fuelDifferences.Count} (≤{maxSmallDiffRecords}) | " +
                    $"Max={maxDifferenceFound:F2}L StdDev={stdDev:F3} → ACCEPTING (Real Refuel)");
            }
        }

        return isUndulation;
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // ENHANCED ANALYZE FUEL DATA WITH IMPROVED UNDULATION DETECTION
    // ─────────────────────────────────────────────────────────────────────────────
    //    static (string report, double totalRefueled) AnalyzeFuelData(
    //List<FuelRecord> records, string jobCode, string assetCode)
    //    {
    //        // ── Tunable thresholds ────────────────────────────────────────────────────
    //        const double minRefuelThreshold = 10.0;      // min fuel gain to count as refuel (L)
    //        const double maxRefuelThreshold = 1200.0;    // sanity cap (L)
    //        const int timeWindowMinutes = 15;            // time-based window (minutes)
    //        const double maxSpeedDuringRefuel = 0.0;     // vehicle must be stationary
    //        const double stabilityBand = 1.0;            // allowed fuel swing in stable window (L)
    //        const int stabilityWindowSec = 180;          // pre/post stability check window (seconds)
    //        const double minFuelChangeToBreakEvent = -0.5; // significant decrease to break event
    //        const double longGapMinutes = 15;            // treat gaps longer than this as potential refuels

    //        // ADVANCED: Undulation detection thresholds
    //        const int maxSmallDiffRecords = 5;           // max records with diff ≤ 1L before classification as undulation
    //        const double smallDiffThreshold = 1.0;       // threshold for small difference (L)
    //        // ──────────────────────────────────────────────────────────────────────────

    //        var reportBuilder = new StringBuilder();
    //        double refuelTotal = 0;

    //        if (records == null || records.Count < 2)
    //            return (string.Empty, refuelTotal);

    //        var recs = records
    //            .Where(r => r.FuelLevel > 0 && r.Rtc != DateTime.MinValue)
    //            .OrderBy(r => r.Rtc)
    //            .ToList();

    //        if (recs.Count < 2)
    //            return (string.Empty, refuelTotal);

    //        int n = recs.Count;
    //        var processedIndices = new HashSet<int>();

    //        // ═══════════════════════════════════════════════════════════════════════════
    //        // PHASE 1: Gap‑based detection (handles long interruptions)
    //        // ═══════════════════════════════════════════════════════════════════════════
    //        for (int i = 0; i < n - 1; i++)
    //        {
    //            if (processedIndices.Contains(i) || processedIndices.Contains(i + 1))
    //                continue;

    //            double gapMinutes = (recs[i + 1].Rtc - recs[i].Rtc).TotalMinutes;
    //            if (gapMinutes <= longGapMinutes)
    //                continue;

    //            double fuelIncrease = recs[i + 1].FuelLevel - recs[i].FuelLevel;
    //            if (fuelIncrease < minRefuelThreshold || fuelIncrease > maxRefuelThreshold)
    //                continue;

    //            if (recs[i].Speed > maxSpeedDuringRefuel || recs[i + 1].Speed > maxSpeedDuringRefuel)
    //                continue;

    //            bool stableBefore = IsStableBeforeTime(recs, i, stabilityWindowSec, stabilityBand);
    //            bool stableAfter = IsStableAfterTime(recs, i + 1, stabilityWindowSec, stabilityBand, n);
    //            string confidence = (stableBefore && stableAfter) ? "High" : (stableBefore || stableAfter) ? "Medium" : "Low";

    //            // ONLY add to total if NOT "Low" confidence
    //            //if (!confidence.StartsWith("Low"))
    //            //{
    //            //    AppendRefuelRow(reportBuilder, assetCode, jobCode,
    //            //                    recs[i], recs[i + 1],
    //            //                    recs[i].FuelLevel, recs[i + 1].FuelLevel,
    //            //                    fuelIncrease, confidence);

    //            //    refuelTotal += fuelIncrease;
    //            //    processedIndices.Add(i);
    //            //    processedIndices.Add(i + 1);
    //            //}
    //        }

    //        // ═══════════════════════════════════════════════════════════════════════════
    //        // PHASE 2: Sequential detection with IMPROVED UNDULATION FILTERING
    //        // ═══════════════════════════════════════════════════════════════════════════
    //        int idx = 0;
    //        while (idx < n)
    //        {
    //            if (processedIndices.Contains(idx))
    //            {
    //                idx++;
    //                continue;
    //            }

    //            if (recs[idx].Speed > maxSpeedDuringRefuel)
    //            {
    //                idx++;
    //                continue;
    //            }

    //            // ─── IMPROVED UNDULATION DETECTION: Count-based analysis ───
    //            if (IsUndulation(recs, idx, maxSmallDiffRecords, smallDiffThreshold))
    //            {
    //                idx++;
    //                continue;
    //            }

    //            int startIdx = idx;
    //            double startFuel = recs[idx].FuelLevel;
    //            int peakIdx = idx;
    //            double peakFuel = recs[idx].FuelLevel;

    //            // Walk backwards to find true minimum
    //            int lookbackIdx = idx;
    //            while (lookbackIdx > 0)
    //            {
    //                double timeSec = (recs[lookbackIdx].Rtc - recs[lookbackIdx - 1].Rtc).TotalSeconds;
    //                if (timeSec > 300) break;
    //                if (recs[lookbackIdx - 1].Speed > maxSpeedDuringRefuel) break;

    //                if (recs[lookbackIdx - 1].FuelLevel < startFuel)
    //                {
    //                    lookbackIdx--;
    //                    startIdx = lookbackIdx;
    //                    startFuel = recs[lookbackIdx].FuelLevel;
    //                }
    //                else
    //                {
    //                    break;
    //                }
    //            }

    //            // ─── CHECK: Skip if this range was already processed in PHASE 1 ───
    //            bool alreadyProcessed = false;
    //            for (int k = startIdx; k <= peakIdx && k < n; k++)
    //            {
    //                if (processedIndices.Contains(k))
    //                {
    //                    alreadyProcessed = true;
    //                    break;
    //                }
    //            }
    //            if (alreadyProcessed)
    //            {
    //                idx++;
    //                continue;
    //            }

    //            // Walk forward to find refuel sequence
    //            int j = startIdx + 1;
    //            bool foundRise = false;

    //            while (j < n)
    //            {
    //                if (recs[j].Speed > maxSpeedDuringRefuel)
    //                    break;

    //                double fuelChange = recs[j].FuelLevel - recs[j - 1].FuelLevel;

    //                if (fuelChange > 0.3)
    //                    foundRise = true;

    //                if (recs[j].FuelLevel > peakFuel)
    //                {
    //                    peakFuel = recs[j].FuelLevel;
    //                    peakIdx = j;
    //                }

    //                if (foundRise && fuelChange < minFuelChangeToBreakEvent)
    //                    break;

    //                j++;

    //                if ((recs[j - 1].Rtc - recs[startIdx].Rtc).TotalMinutes > timeWindowMinutes)
    //                    break;
    //            }

    //            if (!foundRise || peakIdx == startIdx)
    //            {
    //                idx++;
    //                continue;
    //            }

    //            double fuelDiff = peakFuel - startFuel;
    //            if (fuelDiff < minRefuelThreshold || fuelDiff > maxRefuelThreshold)
    //            {
    //                idx++;
    //                continue;
    //            }

    //            TimeSpan duration = recs[peakIdx].Rtc - recs[startIdx].Rtc;
    //            if (duration.TotalSeconds < 30)
    //            {
    //                idx++;
    //                continue;
    //            }

    //            bool stableBefore2 = IsStableBeforeTime(recs, startIdx, stabilityWindowSec, stabilityBand);
    //            bool stableAfter2 = IsStableAfterTime(recs, peakIdx, stabilityWindowSec, stabilityBand, n);

    //            string confidence2 = "Low";
    //            if (stableBefore2 && stableAfter2)
    //                confidence2 = "High";
    //            else if (stableBefore2 || stableAfter2)
    //                confidence2 = "Medium";

    //            Console.WriteLine(
    //                $"[{confidence2.ToUpper()}] Asset={assetCode} | " +
    //                $"{recs[startIdx].Rtc:yyyy-MM-dd HH:mm:ss} → {recs[peakIdx].Rtc:yyyy-MM-dd HH:mm:ss} | " +
    //                $"Start={startFuel:F2}L → Peak={peakFuel:F2}L | Gain={fuelDiff:F2}L | Dur={duration.TotalSeconds:F0}s | " +
    //                $"StableBefore={stableBefore2} StableAfter={stableAfter2}");

    //            // ONLY add to total if NOT "Low" confidence
    //            if (!confidence2.Equals("Low", StringComparison.OrdinalIgnoreCase))
    //            {
    //                const double mediumConfidenceMinThreshold = 17.0;
    //                const double highConfidenceThreshold = 15.0;

    //                bool isValid = (confidence2.Equals("High", StringComparison.OrdinalIgnoreCase) && fuelDiff >= highConfidenceThreshold) ||
    //                               (confidence2.Equals("Medium", StringComparison.OrdinalIgnoreCase) && fuelDiff >= mediumConfidenceMinThreshold);

    //                if (isValid)
    //                {
    //                    AppendRefuelRow(reportBuilder, assetCode, jobCode,
    //                                    recs[startIdx], recs[peakIdx],
    //                                    startFuel, peakFuel, fuelDiff, confidence2);

    //                    refuelTotal += fuelDiff;

    //                    for (int k = startIdx; k <= peakIdx && k < n; k++)
    //                        processedIndices.Add(k);

    //                    idx = peakIdx + 1;
    //                }
    //                else
    //                {
    //                    Console.WriteLine(
    //                        $"[REJECTED - MEDIUM BELOW THRESHOLD] Asset={assetCode} | " +
    //                        $"Gain={fuelDiff:F2}L (requires ≥ {mediumConfidenceMinThreshold}L for Medium confidence)");
    //                    idx++;
    //                }
    //            }
    //            else
    //            {
    //                idx++;
    //            }

    //        }

    //        return (reportBuilder.ToString(), refuelTotal);
    //    }

    static (string report, double totalRefueled) AnalyzeFuelData(
     List<FuelRecord> records, string jobCode, string assetCode,
     DateTime? reportCutoff = null)
    {
        // ═══════════════════════════════════════════════════════════════════════════
        //  REFUEL DETECTION — Unified single-pass algorithm
        //
        //  Design principles:
        //  1.  Find STABLE LEVELS (plateaus), then compare consecutive plateaus.
        //      A refuel is simply: plateau_after − plateau_before ≥ threshold.
        //  2.  A "plateau" is defined as a contiguous window of records where
        //      max−min ≤ stabilityBand, lasting ≥ minPlateauSec seconds.
        //  3.  Plateaus are separated by rises, drops, gaps, or movement.
        //  4.  Raw fuel values are used for BOTH detection and reporting — no
        //      smoothing artifacts.  Median-3 is only used internally to reject
        //      single-sample spikes when deciding if a level is "stable".
        //  5.  No speed filter — refuels are identified by fuel-level shape alone.
        //      Speed was causing systemic misses (GPS reports 1-14 km/h at stops).
        //  6.  Works for gaps (tracker offline) and continuous rises equally — one
        //      algorithm, no separate passes.
        // ═══════════════════════════════════════════════════════════════════════════

        // ── Thresholds ──
        const double minRefuelGain     = 10.0;    // minimum net gain to count (L)
        const double maxRefuelGain     = 1800.0;  // sanity cap (L)
        const double stabilityBand     = 3.0;     // max fuel swing in a plateau (L)
        const int    minPlateauSec     = 50;      // minimum plateau duration (seconds)
        const int    minPlateauRecords = 3;        // minimum records in a plateau
        const double highMinGain       = 10.0;
        const double mediumMinGain     = 18.0;
        const double lowMinGain        = 25.0;

        // ── Post-peak sustainability: reject undulation/sensor noise ──
        // After a real refuel, fuel STAYS at the elevated level.
        // If fuel drops back significantly within this window, it's undulation.
        const double postPeakDropMaxPct = 0.25;  // reject if drop > 25% of gain
        const int    postPeakCheckSec   = 600;   // 10-minute post-peak window

        var reportBuilder = new StringBuilder();
        double refuelTotal = 0;

        if (records == null || records.Count < 2)
            return (string.Empty, refuelTotal);

        var recs = records
            .Where(r => r.FuelLevel > 0 && r.Rtc != DateTime.MinValue)
            .OrderBy(r => r.Rtc)
            .ToList();

        if (recs.Count < 2)
            return (string.Empty, refuelTotal);

        int n = recs.Count;

        // ── Build median-3 smoothed array for spike rejection ──
        var sm = new double[n];
        for (int s = 0; s < n; s++)
        {
            int lo = Math.Max(0, s - 1);
            int hi = Math.Min(n - 1, s + 1);
            double a = recs[lo].FuelLevel, b = recs[s].FuelLevel, c = recs[hi].FuelLevel;
            sm[s] = a < b ? (b < c ? b : (a < c ? c : a))
                          : (a < c ? a : (b < c ? c : b));
        }

        // ═══════════════════════════════════════════════════════════════════════
        //  STEP 1: Identify all stable plateaus
        //
        //  A plateau is the longest contiguous run starting at index i where
        //  max(sm) − min(sm) ≤ stabilityBand.  We greedily extend and record
        //  each plateau as (startIdx, endIdx, medianLevel).
        // ═══════════════════════════════════════════════════════════════════════
        var plateaus = new List<(int startIdx, int endIdx, double level)>();
        int pi = 0;
        while (pi < n)
        {
            double pMin = sm[pi], pMax = sm[pi];
            int pEnd = pi;

            // Extend the plateau as far as possible
            for (int k = pi + 1; k < n; k++)
            {
                double newMin = Math.Min(pMin, sm[k]);
                double newMax = Math.Max(pMax, sm[k]);
                if (newMax - newMin > stabilityBand) break;
                pMin = newMin;
                pMax = newMax;
                pEnd = k;
            }

            double durationSec = (recs[pEnd].Rtc - recs[pi].Rtc).TotalSeconds;
            int recordCount = pEnd - pi + 1;

            if (durationSec >= minPlateauSec && recordCount >= minPlateauRecords)
            {
                // Compute the median raw fuel level of this plateau for reporting
                var rawVals = new List<double>(recordCount);
                for (int k = pi; k <= pEnd; k++)
                    rawVals.Add(recs[k].FuelLevel);
                rawVals.Sort();
                double medianLevel = rawVals[rawVals.Count / 2];

                plateaus.Add((pi, pEnd, medianLevel));
                pi = pEnd + 1;
            }
            else
            {
                pi++;
            }
        }

        // ═══════════════════════════════════════════════════════════════════════
        //  STEP 2a: Non-consecutive plateau comparison — detect STEP-WISE refuels
        //  (runs BEFORE consecutive comparison so step-wise gets priority)
        //
        //  Some vehicles show fuel rising in steps (nozzle filling pattern):
        //    36→46→55→65→77→87   Each step < 10L but total gain = 50L
        //  Consecutive plateau comparison misses these because each step is
        //  below minRefuelGain.
        //
        //  Strategy: for each plateau, scan FORWARD to find the highest
        //  subsequent plateau within a 30-minute window where the total gain
        //  from the start plateau is ≥ minRefuelGain, provided fuel is
        //  generally non-decreasing across the intermediate plateaus.
        // ═══════════════════════════════════════════════════════════════════════
        var detectedEvents = new List<(DateTime start, DateTime end)>();
        const int stepwiseMaxMinutes = 30; // max time window for a step-wise refuel

        for (int pStart = 0; pStart < plateaus.Count - 1; pStart++)
        {
            var startPlat = plateaus[pStart];

            // Skip if this plateau's time range already overlaps a detected event
            bool alreadyCovered = false;
            foreach (var (ds, de) in detectedEvents)
            {
                if (recs[startPlat.startIdx].Rtc <= de && recs[startPlat.endIdx].Rtc >= ds)
                { alreadyCovered = true; break; }
            }
            if (alreadyCovered) continue;

            // Scan forward for the best (highest) endpoint plateau
            int bestEnd = -1;
            double bestGain = 0;

            for (int pEnd2 = pStart + 2; pEnd2 < plateaus.Count; pEnd2++)
            {
                var endPlat = plateaus[pEnd2];

                // Time window check — use end plateau's START (when fuel reached
                // final level), not its END (which includes long settling time)
                double spanMin = (recs[endPlat.startIdx].Rtc - recs[startPlat.endIdx].Rtc).TotalMinutes;
                if (spanMin > stepwiseMaxMinutes) break;

                double totalGain = endPlat.level - startPlat.level;
                if (totalGain < minRefuelGain) continue;

                // Ensure fuel is strictly non-decreasing across intermediate
                // plateaus: each step must be ≥ the previous one.  A drop
                // (e.g. 97→96.5) means the fuel is being consumed, not refuelled,
                // so it should not be part of a step-wise chain.
                bool nonDecreasing = true;
                double prevStepLevel = startPlat.level;
                for (int pm = pStart + 1; pm < pEnd2; pm++)
                {
                    if (plateaus[pm].level < prevStepLevel)
                    { nonDecreasing = false; break; }
                    prevStepLevel = plateaus[pm].level;
                }
                if (!nonDecreasing) continue;

                // Track the best (highest gain) endpoint
                if (totalGain > bestGain)
                {
                    bestGain = totalGain;
                    bestEnd = pEnd2;
                }
            }

            if (bestEnd < 0) continue;

            // Dominance check: in a true step-wise refuel, no single step
            // should account for the vast majority of the total gain.
            // If one step is >80% of the total, it is really a single large
            // refuel with minor precursor noise — let consecutive detection
            // (STEP 2b) handle it instead.
            {
                double maxSingleStep = 0;
                double prevLev = startPlat.level;
                for (int pm = pStart + 1; pm <= bestEnd; pm++)
                {
                    double stepGain = plateaus[pm].level - prevLev;
                    if (stepGain > maxSingleStep) maxSingleStep = stepGain;
                    prevLev = plateaus[pm].level;
                }
                if (maxSingleStep > bestGain * 0.80)
                {
                    Console.WriteLine($"[SKIP-StepwiseDominance] {assetCode} " +
                        $"Plateau {pStart}→{bestEnd}: single step {maxSingleStep:F1}L " +
                        $"= {maxSingleStep / bestGain * 100:F0}% of total {bestGain:F1}L — not step-wise");
                    continue;
                }
            }

            var endPlatBest = plateaus[bestEnd];

            // Find raw trough: search within the start plateau only.
            // (No backward search — pre-plateau data is often noisy driving
            // data with unreliable fuel readings that produce wrong troughs.)
            const double troughSpeedLimit = 2.0;
            int swMinIdx = startPlat.startIdx;
            double swMinVal = recs[startPlat.startIdx].FuelLevel;
            for (int k = startPlat.startIdx + 1; k <= startPlat.endIdx; k++)
            {
                if (recs[k].Speed > troughSpeedLimit) continue;
                if (recs[k].FuelLevel < swMinVal)
                { swMinVal = recs[k].FuelLevel; swMinIdx = k; }
            }

            // Find raw peak in or after end plateau
            int swMaxIdx = endPlatBest.startIdx;
            double swMaxVal = recs[endPlatBest.startIdx].FuelLevel;
            for (int k = endPlatBest.startIdx; k <= endPlatBest.endIdx; k++)
            {
                if (recs[k].FuelLevel > swMaxVal)
                { swMaxVal = recs[k].FuelLevel; swMaxIdx = k; }
            }
            // Also check transition zone between start and end plateau
            for (int k = startPlat.endIdx + 1; k < endPlatBest.startIdx && k < n; k++)
            {
                if (recs[k].FuelLevel > swMaxVal)
                { swMaxVal = recs[k].FuelLevel; swMaxIdx = k; }
            }

            double swRawGain = swMaxVal - swMinVal;
            if (swRawGain < minRefuelGain) continue;

            // Skip if overlaps an already-detected event
            bool swOverlap = false;
            foreach (var (ds, de) in detectedEvents)
            {
                if (recs[swMinIdx].Rtc <= de && recs[swMaxIdx].Rtc >= ds)
                { swOverlap = true; break; }
            }
            if (swOverlap) continue;

            // Cross-date guard
            if (reportCutoff.HasValue && recs[swMinIdx].Rtc > reportCutoff.Value) continue;

            // Post-peak sustainability check
            {
                var swPostCutoff = recs[swMaxIdx].Rtc.AddSeconds(postPeakCheckSec);
                var swPostVals = new List<double>();
                for (int k = swMaxIdx + 1; k < n; k++)
                {
                    if (recs[k].Rtc > swPostCutoff) break;
                    swPostVals.Add(recs[k].FuelLevel);
                }
                if (swPostVals.Count >= 3)
                {
                    double swPostMin = swPostVals.Min();
                    double swDrop = swMaxVal - swPostMin;
                    if (swDrop > swRawGain * postPeakDropMaxPct)
                    {
                        Console.WriteLine($"[REJECTED-PostDrop-Stepwise] {assetCode} " +
                            $"{recs[swMinIdx].Rtc:MM-dd HH:mm} → {recs[swMaxIdx].Rtc:MM-dd HH:mm} " +
                            $"Gain={swRawGain:F2}L PostMin={swPostMin:F2} Drop={swDrop:F2}L " +
                            $"({swDrop / swRawGain * 100:F0}% of gain) — undulation");
                        continue;
                    }
                }
            }

            // Speed validation
            {
                bool hasGap = false;
                for (int k = swMinIdx; k < swMaxIdx; k++)
                {
                    if ((recs[k + 1].Rtc - recs[k].Rtc).TotalMinutes >= 5.0)
                    { hasGap = true; break; }
                }
                if (!hasGap)
                {
                    double spd1Sum = 0; int spd1Cnt = 0;
                    for (int k = startPlat.startIdx; k <= startPlat.endIdx; k++)
                    { spd1Sum += recs[k].Speed; spd1Cnt++; }
                    double spd2Sum = 0; int spd2Cnt = 0;
                    for (int k = endPlatBest.startIdx; k <= endPlatBest.endIdx; k++)
                    { spd2Sum += recs[k].Speed; spd2Cnt++; }
                    double avg1 = spd1Cnt > 0 ? spd1Sum / spd1Cnt : 0;
                    double avg2 = spd2Cnt > 0 ? spd2Sum / spd2Cnt : 0;
                    if (avg1 > 3.0 && avg2 > 3.0)
                    {
                        Console.WriteLine($"[REJECTED-Speed-Stepwise] {assetCode} " +
                            $"{recs[swMinIdx].Rtc:MM-dd HH:mm} → {recs[swMaxIdx].Rtc:MM-dd HH:mm} " +
                            $"Gain={swRawGain:F2}L — both plateaus moving");
                        continue;
                    }
                }
            }

            // Dip-recovery check
            if (swMinIdx > 2)
            {
                var swLookback = recs[swMinIdx].Rtc.AddMinutes(-10);
                var swCtxVals = new List<double>();
                for (int k = swMinIdx - 1; k >= 0; k--)
                {
                    if (recs[k].Rtc < swLookback) break;
                    swCtxVals.Add(recs[k].FuelLevel);
                }
                if (swCtxVals.Count >= 3)
                {
                    double swCtxMax = swCtxVals.Max();
                    if (swCtxMax >= swMaxVal - stabilityBand)
                    {
                        Console.WriteLine($"[REJECTED-DipRecovery-Stepwise] {assetCode} " +
                            $"{recs[swMinIdx].Rtc:MM-dd HH:mm} → {recs[swMaxIdx].Rtc:MM-dd HH:mm} " +
                            $"Gain={swRawGain:F2}L contextMax={swCtxMax:F2} peak={swMaxVal:F2}");
                        continue;
                    }
                }
            }

            // Confidence
            double swPrevRange = 0, swNextRange = 0;
            {
                double mi = recs[startPlat.startIdx].FuelLevel, ma = mi;
                for (int k = startPlat.startIdx + 1; k <= startPlat.endIdx; k++)
                { if (recs[k].FuelLevel < mi) mi = recs[k].FuelLevel; if (recs[k].FuelLevel > ma) ma = recs[k].FuelLevel; }
                swPrevRange = ma - mi;
            }
            {
                double mi = recs[endPlatBest.startIdx].FuelLevel, ma = mi;
                for (int k = endPlatBest.startIdx + 1; k <= endPlatBest.endIdx; k++)
                { if (recs[k].FuelLevel < mi) mi = recs[k].FuelLevel; if (recs[k].FuelLevel > ma) ma = recs[k].FuelLevel; }
                swNextRange = ma - mi;
            }
            bool swPrevTight = swPrevRange <= stabilityBand && (recs[startPlat.endIdx].Rtc - recs[startPlat.startIdx].Rtc).TotalSeconds >= minPlateauSec;
            bool swNextTight = swNextRange <= stabilityBand && (recs[endPlatBest.endIdx].Rtc - recs[endPlatBest.startIdx].Rtc).TotalSeconds >= minPlateauSec;

            string swConf = (swPrevTight && swNextTight) ? "High"
                          : (swPrevTight || swNextTight) ? "Medium"
                          : "Low";

            // Confidence boost factor 1: Step count — the hallmark of step-wise
            // refuelling. 4+ plateaus (≥3 transitions) is a strong step-wise
            // signature that doesn't happen from sensor noise.
            int swStepCount = bestEnd - pStart; // number of plateau transitions
            if (swStepCount >= 3)
            {
                if (swConf == "Low") swConf = "Medium";
                else if (swConf == "Medium") swConf = "High";
            }

            // Confidence boost factor 2: Very large gain is unambiguous —
            // a 40+ L jump across multiple plateaus is never sensor noise,
            // regardless of plateau tightness.
            if (swRawGain >= 40.0)
            {
                if (swConf == "Low") swConf = "Medium";
                else if (swConf == "Medium") swConf = "High";
            }

            double swThreshold = swConf == "High" ? highMinGain
                               : swConf == "Medium" ? mediumMinGain
                               : lowMinGain;
            if (swRawGain < swThreshold) continue;

            Console.WriteLine($"[{swConf}-Stepwise] {assetCode} {recs[swMinIdx].Rtc:MM-dd HH:mm} → {recs[swMaxIdx].Rtc:MM-dd HH:mm} " +
                              $"Trough={swMinVal:F2} Peak={swMaxVal:F2} Gain={swRawGain:F2}L (across {bestEnd - pStart + 1} plateaus)");

            AppendRefuelRow(reportBuilder, assetCode, jobCode,
                recs[swMinIdx], recs[swMaxIdx],
                swMinVal, swMaxVal, swRawGain, swConf);

            refuelTotal += swRawGain;
            detectedEvents.Add((recs[swMinIdx].Rtc, recs[swMaxIdx].Rtc));
        }

        // ═══════════════════════════════════════════════════════════════════════
        //  STEP 2b: Compare consecutive plateaus to detect refuels
        //
        //  If plateau[i+1].level − plateau[i].level ≥ threshold → refuel.
        //  We also search between the two plateaus for the true raw min (trough)
        //  and true raw max (peak) to get accurate start/end values and times.
        //  Skips events already found by step-wise detection above.
        // ═══════════════════════════════════════════════════════════════════════

        for (int p = 0; p < plateaus.Count - 1; p++)
        {
            var prev = plateaus[p];
            var next = plateaus[p + 1];

            double gain = next.level - prev.level;
            if (gain < minRefuelGain || gain > maxRefuelGain) continue;

            // NOTE: overlap check moved below (after trough/peak search)
            // so we use precise event times, not broad plateau boundaries.

            // Find the true raw trough (minimum) in plateau[p] and the gap between
            // Only consider STATIONARY records (speed ≤ 2) to avoid fuel sloshing noise
            int searchStart = prev.startIdx;
            int searchEnd   = next.endIdx;

            // Find raw min in [prev.startIdx .. next.startIdx-1]
            int rawMinIdx = prev.startIdx;
            double rawMinVal = recs[prev.startIdx].FuelLevel;
            for (int k = prev.startIdx + 1; k < next.startIdx && k < n; k++)
            {
                if (recs[k].Speed >=1) continue; // skip moving records
                if (recs[k].FuelLevel < rawMinVal)
                {
                    rawMinVal = recs[k].FuelLevel;
                    rawMinIdx = k;
                }
            }

            // Find raw max in [rawMinIdx+1 .. next.endIdx]
            // IMPORTANT: Stop at the first record where the vehicle is clearly
            // moving (speed > 2 km/h).  The true refuel peak is the last
            // stationary reading before drive-away; subsequent readings include
            // driving slosh / sensor noise at speed that inflate the peak.
            // If no stationary peak is found in the gap, fall back to the
            // next-plateau median (which is by definition stable).
            const double peakSpeedLimit = 1.0; // km/h — GPS commonly reports 5-6 at stops
            int rawMaxIdx = rawMinIdx + 1 < n ? rawMinIdx + 1 : rawMinIdx;
            double rawMaxVal = rawMaxIdx < n ? recs[rawMaxIdx].FuelLevel : recs[rawMinIdx].FuelLevel;

            // First scan: find highest fuel in the transition zone.
            // Skip records with high GPS speed but don't stop scanning —
            // GPS noise at stops commonly reports 5-6 km/h.
            for (int k = rawMinIdx + 1; k < next.startIdx && k < n; k++)
            {
                if (recs[k].Speed > peakSpeedLimit) continue; // skip noisy GPS records
                if (recs[k].FuelLevel >= rawMaxVal)
                {
                    rawMaxVal = recs[k].FuelLevel;
                    rawMaxIdx = k;
                }
            }

            // Second scan: also check within the next plateau itself.
            // The next plateau may contain the actual peak value.
            for (int k = next.startIdx; k <= next.endIdx && k < n; k++)
            {
                if (recs[k].Speed > peakSpeedLimit) continue; // skip records with high speed
                if (recs[k].FuelLevel > rawMaxVal)
                {
                    rawMaxVal = recs[k].FuelLevel;
                    rawMaxIdx = k;
                }
            }

            double rawGain = rawMaxVal - rawMinVal;
            if (rawGain < minRefuelGain) continue;

            // Skip if the actual trough→peak time range overlaps a detected event
            {
                bool eventOverlap = false;
                foreach (var (ds, de) in detectedEvents)
                {
                    if (recs[rawMinIdx].Rtc < de && recs[rawMaxIdx].Rtc > ds)
                    { eventOverlap = true; break; }
                }
                if (eventOverlap) continue;
            }

            // Cross-date guard
            if (reportCutoff.HasValue && recs[rawMinIdx].Rtc > reportCutoff.Value) continue;

            // ── Speed validation: reject fuel sloshing while driving ──
            // A real refuel means the vehicle STOPPED to get fuel.  The flanking
            // plateaus represent the fuel level before and after the event.
            // If BOTH plateaus show the vehicle was moving (avg speed > threshold),
            // the "gain" is just the sensor reading different sloshing levels at
            // different speeds / terrain — not a real refuel.
            //
            // At a genuine fuelling station, at least ONE plateau (usually the
            // pre-plateau) will have the vehicle stationary (avg speed ≤ 3 km/h).
            {
                // Check for large data gap between trough & peak (refuel during blackout)
                bool hasDataGap = false;
                for (int k = rawMinIdx; k < rawMaxIdx; k++)
                {
                    if ((recs[k + 1].Rtc - recs[k].Rtc).TotalMinutes >= 5.0)
                    { hasDataGap = true; break; }
                }

                if (!hasDataGap)
                {
                    // Compute average speed during each flanking plateau
                    double prevPlateauSpeedSum = 0; int prevPlateauCnt = 0;
                    for (int k = prev.startIdx; k <= prev.endIdx; k++)
                    { prevPlateauSpeedSum += recs[k].Speed; prevPlateauCnt++; }

                    double nextPlateauSpeedSum = 0; int nextPlateauCnt = 0;
                    for (int k = next.startIdx; k <= next.endIdx; k++)
                    { nextPlateauSpeedSum += recs[k].Speed; nextPlateauCnt++; }

                    double prevAvgSpd = prevPlateauCnt > 0 ? prevPlateauSpeedSum / prevPlateauCnt : 0;
                    double nextAvgSpd = nextPlateauCnt > 0 ? nextPlateauSpeedSum / nextPlateauCnt : 0;

                    const double plateauMovingThreshold = 3.0; // km/h
                    if (prevAvgSpd > plateauMovingThreshold && nextAvgSpd > plateauMovingThreshold)
                    {
                        Console.WriteLine($"[REJECTED-PlateauSpeed] {assetCode} " +
                            $"{recs[rawMinIdx].Rtc:MM-dd HH:mm} → {recs[rawMaxIdx].Rtc:MM-dd HH:mm} " +
                            $"Gain={rawGain:F2}L prevPlateauSpd={prevAvgSpd:F1} nextPlateauSpd={nextAvgSpd:F1} — both plateaus moving");
                        continue;
                    }
                }
            }

            // ── Dip-recovery check ──
            // If the fuel level in the 10 minutes BEFORE the pre-plateau was
            // already near the post-plateau peak, the "gain" is just a sensor
            // dip that recovered — not a real refuel.
            if (prev.startIdx > 2)
            {
                var lookbackCutoff = recs[prev.startIdx].Rtc.AddMinutes(-10);
                var contextVals = new List<double>();
                for (int k = prev.startIdx - 1; k >= 0; k--)
                {
                    if (recs[k].Rtc < lookbackCutoff) break;
                    contextVals.Add(recs[k].FuelLevel);
                }
                if (contextVals.Count >= 3)
                {
                    double contextMax = contextVals.Max();
                    // If the fuel was already at (or above) the peak level recently,
                    // the trough was a temporary sensor dip that recovered
                    if (contextMax >= rawMaxVal - stabilityBand)
                    {
                        Console.WriteLine($"[REJECTED-DipRecovery] {assetCode} " +
                            $"{recs[rawMinIdx].Rtc:MM-dd HH:mm} → {recs[rawMaxIdx].Rtc:MM-dd HH:mm} " +
                            $"Gain={rawGain:F2}L contextMax={contextMax:F2} peak={rawMaxVal:F2} — fuel dip recovery");
                        continue;
                    }
                }
            }

            // ── Post-peak sustainability check ──
            // After a real refuel, fuel STAYS at the elevated level.
            // If fuel drops back down within 5 minutes, it's undulation.
            {
                var postCheckCutoff = recs[rawMaxIdx].Rtc.AddSeconds(postPeakCheckSec);
                var postPeakVals = new List<double>();
                for (int k = rawMaxIdx + 1; k < n; k++)
                {
                    if (recs[k].Rtc > postCheckCutoff) break;
                    postPeakVals.Add(recs[k].FuelLevel);
                }
                if (postPeakVals.Count >= 3)
                {
                    double postMin = postPeakVals.Min();
                    double dropFromPeak = rawMaxVal - postMin;
                    if (dropFromPeak > rawGain * postPeakDropMaxPct)
                    {
                        Console.WriteLine($"[REJECTED-PostDrop] {assetCode} " +
                            $"{recs[rawMinIdx].Rtc:MM-dd HH:mm} → {recs[rawMaxIdx].Rtc:MM-dd HH:mm} " +
                            $"Gain={rawGain:F2}L PostMin={postMin:F2} Drop={dropFromPeak:F2}L " +
                            $"({dropFromPeak / rawGain * 100:F0}% of gain) — undulation");
                        continue;
                    }
                }
            }

            // ── Confidence: now based on plateau quality ──
            // Both plateaus exist by definition (≥60s, ≥3 records), so we check
            // their raw range for tighter stability.
            double prevRange = 0, nextRange = 0;
            {
                double pMi = recs[prev.startIdx].FuelLevel, pMa = pMi;
                for (int k = prev.startIdx + 1; k <= prev.endIdx; k++)
                {
                    if (recs[k].FuelLevel < pMi) pMi = recs[k].FuelLevel;
                    if (recs[k].FuelLevel > pMa) pMa = recs[k].FuelLevel;
                }
                prevRange = pMa - pMi;
            }
            {
                double pMi = recs[next.startIdx].FuelLevel, pMa = pMi;
                for (int k = next.startIdx + 1; k <= next.endIdx; k++)
                {
                    if (recs[k].FuelLevel < pMi) pMi = recs[k].FuelLevel;
                    if (recs[k].FuelLevel > pMa) pMa = recs[k].FuelLevel;
                }
                nextRange = pMa - pMi;
            }

            bool prevTight = prevRange <= stabilityBand && (recs[prev.endIdx].Rtc - recs[prev.startIdx].Rtc).TotalSeconds >= minPlateauSec;
            bool nextTight = nextRange <= stabilityBand && (recs[next.endIdx].Rtc - recs[next.startIdx].Rtc).TotalSeconds >= minPlateauSec;

            string confidence = (prevTight && nextTight) ? "High"
                              : (prevTight || nextTight) ? "Medium"
                              : "Low";

            // ── Gain-based confidence boost ──
            // A 40+ L jump between two consecutive plateaus is unambiguously a
            // real refuel regardless of plateau tightness — sensor noise never
            // produces sustained gains that large.
            if (rawGain >= 20.0)
            {
                if (confidence == "Low")  confidence = "Medium";
                else if (confidence == "Medium") confidence = "High";
            }

            double gainThreshold = confidence == "High"   ? highMinGain
                                 : confidence == "Medium" ? mediumMinGain
                                 : lowMinGain;

            if (rawGain < gainThreshold) continue;

            Console.WriteLine($"[{confidence}] {assetCode} {recs[rawMinIdx].Rtc:MM-dd HH:mm} → {recs[rawMaxIdx].Rtc:MM-dd HH:mm} " +
                              $"Trough={rawMinVal:F2} Peak={rawMaxVal:F2} Gain={rawGain:F2}L");

            AppendRefuelRow(reportBuilder, assetCode, jobCode,
                recs[rawMinIdx], recs[rawMaxIdx],
                rawMinVal, rawMaxVal, rawGain, confidence);

            refuelTotal += rawGain;
            detectedEvents.Add((recs[rawMinIdx].Rtc, recs[rawMaxIdx].Rtc));
        }

        // ═══════════════════════════════════════════════════════════════════════
        //  STEP 3: Supplementary detection — catch refuels that the plateau
        //  comparison misses because:
        //   (a) Pre-refuel data is too noisy/short to form a valid plateau
        //   (b) There is a large time gap during the actual refueling
        //   (c) Data starts during a refuel (no before-plateau at all)
        //
        //  Strategy:
        //   3a. If records exist before the first plateau and their median level
        //       is significantly lower → refuel from baseline to first plateau.
        //   3b. Detect large time gaps (≥5 min); compare fuel medians on each
        //       side of the gap.
        // ═══════════════════════════════════════════════════════════════════════

        // Helper: check if a time range overlaps any already-detected event
        bool OverlapsDetected(DateTime s, DateTime e)
        {
            foreach (var (ds, de) in detectedEvents)
                if (s <= de && e >= ds) return true;
            return false;
        }

        // ── 3a: Pre-first-plateau check ──
        if (plateaus.Count > 0 && plateaus[0].startIdx > 1)
        {
            var firstP = plateaus[0];
            // Gather raw fuel levels of all records before the first plateau
            var preVals = new List<double>();
            for (int k = 0; k < firstP.startIdx; k++)
                preVals.Add(recs[k].FuelLevel);

            if (preVals.Count >= 2)
            {
                preVals.Sort();
                double baseline = preVals[preVals.Count / 2]; // median
                double gapGain = firstP.level - baseline;

                if (gapGain >= minRefuelGain && gapGain <= maxRefuelGain)
                {
                    // Find the trough (raw min) among pre-plateau records
                    int troughIdx = 0;
                    for (int k = 1; k < firstP.startIdx; k++)
                        if (recs[k].FuelLevel < recs[troughIdx].FuelLevel) troughIdx = k;

                    // Find the peak (raw max) within the first plateau
                    int peakIdx3a = firstP.startIdx;
                    for (int k = firstP.startIdx + 1; k <= firstP.endIdx; k++)
                        if (recs[k].FuelLevel > recs[peakIdx3a].FuelLevel) peakIdx3a = k;

                    double rawGain3a = recs[peakIdx3a].FuelLevel - recs[troughIdx].FuelLevel;

                    if (rawGain3a >= minRefuelGain && rawGain3a <= maxRefuelGain
                        && !OverlapsDetected(recs[troughIdx].Rtc, recs[peakIdx3a].Rtc)
                        && !(reportCutoff.HasValue && recs[troughIdx].Rtc > reportCutoff.Value))
                    {
                        // ── Speed validation: check if the first plateau shows stopped ──
                        // For pre-plateau events, the "after" plateau is firstP.
                        // If the first plateau shows vehicle was moving, reject.
                        bool reject3a = false;
                        {
                            bool gap3a = false;
                            for (int k = troughIdx; k < peakIdx3a; k++)
                            {
                                if ((recs[k + 1].Rtc - recs[k].Rtc).TotalMinutes >= 5.0)
                                { gap3a = true; break; }
                            }
                            if (!gap3a)
                            {
                                double fpSpeedSum = 0; int fpCnt = 0;
                                for (int k = firstP.startIdx; k <= firstP.endIdx; k++)
                                { fpSpeedSum += recs[k].Speed; fpCnt++; }
                                double fpAvg = fpCnt > 0 ? fpSpeedSum / fpCnt : 0;
                                // Also check pre-trough records
                                double preSpeedSum = 0; int preCnt = 0;
                                for (int k = 0; k < firstP.startIdx; k++)
                                { preSpeedSum += recs[k].Speed; preCnt++; }
                                double preAvg = preCnt > 0 ? preSpeedSum / preCnt : 0;
                                if (preAvg > 3.0 && fpAvg > 3.0) reject3a = true;
                            }
                        }
                        if (reject3a)
                        {
                            Console.WriteLine($"[REJECTED-Speed-PrePlateau] {assetCode} " +
                                $"{recs[troughIdx].Rtc:MM-dd HH:mm} → {recs[peakIdx3a].Rtc:MM-dd HH:mm} " +
                                $"Gain={rawGain3a:F2}L — vehicle was moving");
                        }
                        // ── Post-peak sustainability check (3a) ──
                        else if (true)
                        {
                            var postCutoff3a = recs[peakIdx3a].Rtc.AddSeconds(postPeakCheckSec);
                            var postVals3a = new List<double>();
                            for (int k = peakIdx3a + 1; k < n; k++)
                            {
                                if (recs[k].Rtc > postCutoff3a) break;
                                postVals3a.Add(recs[k].FuelLevel);
                            }
                            bool postDrop3a = false;
                            if (postVals3a.Count >= 3)
                            {
                                double postMin3a = postVals3a.Min();
                                double drop3a = recs[peakIdx3a].FuelLevel - postMin3a;
                                if (drop3a > rawGain3a * postPeakDropMaxPct)
                                {
                                    postDrop3a = true;
                                    Console.WriteLine($"[REJECTED-PostDrop-PrePlateau] {assetCode} " +
                                        $"{recs[troughIdx].Rtc:MM-dd HH:mm} → {recs[peakIdx3a].Rtc:MM-dd HH:mm} " +
                                        $"Gain={rawGain3a:F2}L PostMin={postMin3a:F2} Drop={drop3a:F2}L " +
                                        $"({drop3a / rawGain3a * 100:F0}% of gain) — undulation");
                                }
                            }
                            if (!postDrop3a)
                        {
                        string conf3a = rawGain3a >= 25 ? "High" : rawGain3a >= 15 ? "Medium" : "Low";
                        // Boost confidence if gain is large
                        double dur3a = (recs[peakIdx3a].Rtc - recs[troughIdx].Rtc).TotalMinutes;
                        if (rawGain3a >= 25 || dur3a >= 30)
                            conf3a = conf3a == "Low" ? "Medium" : "High";

                        Console.WriteLine($"[{conf3a}-PrePlateau] {assetCode} " +
                            $"{recs[troughIdx].Rtc:MM-dd HH:mm} → {recs[peakIdx3a].Rtc:MM-dd HH:mm} " +
                            $"Trough={recs[troughIdx].FuelLevel:F2} Peak={recs[peakIdx3a].FuelLevel:F2} Gain={rawGain3a:F2}L");

                        AppendRefuelRow(reportBuilder, assetCode, jobCode,
                            recs[troughIdx], recs[peakIdx3a],
                            recs[troughIdx].FuelLevel, recs[peakIdx3a].FuelLevel, rawGain3a, conf3a);

                        refuelTotal += rawGain3a;
                        detectedEvents.Add((recs[troughIdx].Rtc, recs[peakIdx3a].Rtc));
                        } // end if (!postDrop3a)
                        } // end post-peak sustainability check (3a)
                    }
                }
            }
        }

        // ── 3b: Gap-based detection ──
        //    A large time gap (≥5 min) with a significant fuel jump across it
        //    is a strong indicator of refueling during sensor blackout.
        const double gapMinutes = 5.0;
        const int    gapSampleSize = 5; // records on each side to compute median

        for (int gi = 1; gi < n; gi++)
        {
            double gap = (recs[gi].Rtc - recs[gi - 1].Rtc).TotalMinutes;
            if (gap < gapMinutes) continue;

            // Median of up to gapSampleSize records BEFORE the gap
            int preStart = Math.Max(0, gi - gapSampleSize);
            var preGap = new List<double>();
            for (int k = preStart; k < gi; k++)
                preGap.Add(recs[k].FuelLevel);
            preGap.Sort();
            double preMedian = preGap[preGap.Count / 2];

            // Median of up to gapSampleSize records AFTER the gap
            int postEnd = Math.Min(n, gi + gapSampleSize);
            var postGap = new List<double>();
            for (int k = gi; k < postEnd; k++)
                postGap.Add(recs[k].FuelLevel);
            postGap.Sort();
            double postMedian = postGap[postGap.Count / 2];

            double gapGain = postMedian - preMedian;
            if (gapGain < minRefuelGain || gapGain > maxRefuelGain) continue;

            // Find trough (raw min) in the pre-gap window
            int gTroughIdx = preStart;
            for (int k = preStart + 1; k < gi; k++)
                if (recs[k].FuelLevel < recs[gTroughIdx].FuelLevel) gTroughIdx = k;

            // Find peak (raw max) in the post-gap window
            int gPeakIdx = gi;
            for (int k = gi + 1; k < postEnd; k++)
                if (recs[k].FuelLevel > recs[gPeakIdx].FuelLevel) gPeakIdx = k;

            double gRawGain = recs[gPeakIdx].FuelLevel - recs[gTroughIdx].FuelLevel;
            if (gRawGain < minRefuelGain) continue;

            // Skip if already detected by plateau comparison or pre-plateau check
            if (OverlapsDetected(recs[gTroughIdx].Rtc, recs[gPeakIdx].Rtc)) continue;

            // Cross-date guard
            if (reportCutoff.HasValue && recs[gTroughIdx].Rtc > reportCutoff.Value) continue;

            // ── Post-peak sustainability check (3b) ──
            {
                var postCutoff3b = recs[gPeakIdx].Rtc.AddSeconds(postPeakCheckSec);
                var postVals3b = new List<double>();
                for (int k = gPeakIdx + 1; k < n; k++)
                {
                    if (recs[k].Rtc > postCutoff3b) break;
                    postVals3b.Add(recs[k].FuelLevel);
                }
                if (postVals3b.Count >= 3)
                {
                    double postMin3b = postVals3b.Min();
                    double drop3b = recs[gPeakIdx].FuelLevel - postMin3b;
                    if (drop3b > gRawGain * postPeakDropMaxPct)
                    {
                        Console.WriteLine($"[REJECTED-PostDrop-GapDetect] {assetCode} " +
                            $"{recs[gTroughIdx].Rtc:MM-dd HH:mm} → {recs[gPeakIdx].Rtc:MM-dd HH:mm} " +
                            $"Gain={gRawGain:F2}L PostMin={postMin3b:F2} Drop={drop3b:F2}L " +
                            $"({drop3b / gRawGain * 100:F0}% of gain) — undulation");
                        continue;
                    }
                }
            }

            string gConf = gRawGain >= 25 ? "High" : gRawGain >= 15 ? "Medium" : "Low";
            double gDur = (recs[gPeakIdx].Rtc - recs[gTroughIdx].Rtc).TotalMinutes;
            if (gRawGain >= 25 || gDur >= 30)
                gConf = gConf == "Low" ? "Medium" : "High";

            Console.WriteLine($"[{gConf}-GapDetect] {assetCode} " +
                $"{recs[gTroughIdx].Rtc:MM-dd HH:mm} → {recs[gPeakIdx].Rtc:MM-dd HH:mm} " +
                $"Trough={recs[gTroughIdx].FuelLevel:F2} Peak={recs[gPeakIdx].FuelLevel:F2} Gain={gRawGain:F2}L (gap={gap:F1}min)");

            AppendRefuelRow(reportBuilder, assetCode, jobCode,
                recs[gTroughIdx], recs[gPeakIdx],
                recs[gTroughIdx].FuelLevel, recs[gPeakIdx].FuelLevel, gRawGain, gConf);

            refuelTotal += gRawGain;
            detectedEvents.Add((recs[gTroughIdx].Rtc, recs[gPeakIdx].Rtc));
        }

        return (reportBuilder.ToString(), refuelTotal);
    }

    // ═══ PLATEAU DETECTION HELPER ═══
    static int DetectPlateauEnd(List<FuelRecord> recs, int peakIdx,
        double plateauTolerance, int minPlateauPoints, int n)
    {
        double peakFuel = recs[peakIdx].FuelLevel;
        int plateauCount = 0;
        int endIdx = peakIdx;

        // Look ahead to find where plateau ends
        for (int i = peakIdx + 1; i < Math.Min(peakIdx + 20, n); i++)
        {
            double diff = Math.Abs(recs[i].FuelLevel - peakFuel);

            if (diff <= plateauTolerance)
            {
                plateauCount++;
                endIdx = i;

                if (plateauCount >= minPlateauPoints)
                    break;
            }
            else if (recs[i].FuelLevel > peakFuel)
            {
                // Fuel still rising - update peak
                peakFuel = recs[i].FuelLevel;
                endIdx = i;
                plateauCount = 0;
            }
            else
            {
                // Fuel dropping after peak - we're done
                break;
            }
        }

        return endIdx;
    }




    // ─────────────────────────────────────────────────────────────────────────────
    // Helper methods (unchanged)
    // ─────────────────────────────────────────────────────────────────────────────
    static bool IsStableBeforeTime(
        List<FuelRecord> recs, int startIdx,
        int windowSec, double band)
    {
        if (startIdx <= 0) return true;

        var cutoff = recs[startIdx].Rtc.AddSeconds(-windowSec);
        double min = recs[startIdx].FuelLevel, max = recs[startIdx].FuelLevel;
        int count = 1;
        // Walk backwards from startIdx — stops as soon as we exit the window
        for (int k = startIdx - 1; k >= 0; k--)
        {
            if (recs[k].Rtc < cutoff) break;
            double f = recs[k].FuelLevel;
            if (f < min) min = f;
            if (f > max) max = f;
            count++;
        }
        if (count < 2) return true;
        return (max - min) <= band;
    }

    static bool IsStableAfterTime(
        List<FuelRecord> recs, int peakIdx,
        int windowSec, double band, int n)
    {
        if (peakIdx >= n - 1) return true;

        var cutoff = recs[peakIdx].Rtc.AddSeconds(windowSec);
        double min = recs[peakIdx].FuelLevel, max = recs[peakIdx].FuelLevel;
        int count = 1;
        // Walk forwards from peakIdx — stops as soon as we exit the window
        for (int k = peakIdx + 1; k < n; k++)
        {
            if (recs[k].Rtc > cutoff) break;
            double f = recs[k].FuelLevel;
            if (f < min) min = f;
            if (f > max) max = f;
            count++;
        }
        if (count < 2) return true;
        return (max - min) <= band;
    }

    static void AppendRefuelRow(
     StringBuilder report,
     string assetCode,
     string jobCode,
     FuelRecord start,
     FuelRecord peak,
     double startFuel,
     double peakFuel,
     double fuelDiff,
     string confidence)
    {
        // SKIP "Low" confidence refuels - only add Medium and High to email report
        //if (confidence.Equals("Low", StringComparison.OrdinalIgnoreCase))
        //{
        //    Console.WriteLine($"[SKIPPED - LOW CONFIDENCE] Asset={assetCode} | {start.Rtc:yyyy-MM-dd HH:mm:ss} → {peak.Rtc:yyyy-MM-dd HH:mm:ss} | Gain={fuelDiff:F2}L");
        //    return; // Don't add to report
        //}

        string rowStyle = confidence switch
        {
            "Medium" => "background-color:#e2f0ff",
            "High" => "",
            _ => ""
        };

        report.AppendLine($"<tr style='{rowStyle}'>");
        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{assetCode}</td>");
        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{jobCode}</td>");
        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{start.Rtc:yyyy-MM-dd HH:mm:ss}</td>");
        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{peak.Rtc:yyyy-MM-dd HH:mm:ss}</td>");
        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{startFuel:F2}</td>");
        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{peakFuel:F2}</td>");
        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{fuelDiff:F2}</td>");
        report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>Refuel ({confidence})</td>");
        report.AppendLine("</tr>");
    }

    //public static (string report, double totalRefueled) AnalyzeFuelData(
    //  List<FuelRecord> records, string jobCode, string assetCode)
    //{
    //    var sb = new StringBuilder();
    //    double total = 0;

    //    // ── Tunable thresholds ────────────────────────────────────────────────
    //    const double minRefuelThreshold = 10.0;   // min gain to count (L) — lowered
    //    const double maxRefuelThreshold = 1200.0; // sanity cap (L)
    //    const int timeWindowMinutes = 15;     // max event window (min)
    //    const double maxSpeedDuringRefuel = 0.0;    // vehicle must be stationary
    //    const double stabilityBand = 2.0;    // pre/post stability band (L)
    //    const int stabilityWindowSec = 60;     // pre/post stability window (s)
    //    const double minFuelChangeToBreakEvent = -1.0;  // relaxed: don't break on small dips
    //    const int minLookbackSec = 300;    // 5-min window for min-fuel search
    //    const double peakStabilityBand = 0.5;    // relaxed peak tolerance (L)
    //    const int peakStabilityWinSec = 60;     // stable-peak confirmation window (s)
    //    const double riseStartDelta = 0.2;    // single-step rise threshold (L)
    //    const int minRiseSamples = 2;      // consecutive rising steps needed
    //    const double minCumulativeRise = 0.5;    // OR cumulative gain threshold (L)

    //    // ── Tiered validation thresholds ─────────────────────────────────────
    //    // HIGH confidence  → accept gain >= 10L,  duration >= 60s
    //    // MEDIUM confidence → accept gain >= 25L,  duration >= 120s
    //    // LOW confidence   → reject always
    //    const double highConfMinGain = 10.0;
    //    const int highConfMinDurSec = 60;
    //    const double medConfMinGain = 25.0;
    //    const int medConfMinDurSec = 120;
    //    // ─────────────────────────────────────────────────────────────────────

    //    if (records == null || records.Count < 2)
    //        return (string.Empty, total);

    //    // 1. Clean + sort
    //    var recs = records
    //        .Where(r => r.FuelLevel > 0 && r.Rtc != DateTime.MinValue)
    //        .OrderBy(r => r.Rtc)
    //        .ToList();

    //    int n = recs.Count;
    //    if (n < 2) return (string.Empty, total);

    //    // 2. Rolling-median smooth (window=3) — suppresses 1-sample spikes
    //    int half = 1;
    //    var smoothed = new List<double>(n);
    //    for (int s = 0; s < n; s++)
    //    {
    //        int lo = Math.Max(0, s - half);
    //        int hi = Math.Min(n - 1, s + half);
    //        var slice = recs.GetRange(lo, hi - lo + 1)
    //                        .Select(r => r.FuelLevel)
    //                        .OrderBy(x => x)
    //                        .ToList();
    //        smoothed.Add(slice[slice.Count / 2]);
    //    }

    //    var processed = new HashSet<int>();
    //    int i = 0;

    //    while (i < n)
    //    {
    //        if (processed.Contains(i) || recs[i].Speed > maxSpeedDuringRefuel)
    //        { i++; continue; }

    //        // ── PHASE 1: Detect a confirmed rise starting at/after index i ────
    //        int riseStartIdx = -1;

    //        for (int si = i; si < n - minRiseSamples; si++)
    //        {
    //            if (processed.Contains(si) || recs[si].Speed > maxSpeedDuringRefuel)
    //                continue;

    //            int consecutive = 0;
    //            double cumulative = 0;
    //            var riseWindow = recs[si].Rtc.AddSeconds(minLookbackSec);

    //            for (int sj = si + 1; sj < n && recs[sj].Rtc <= riseWindow; sj++)
    //            {
    //                double step = smoothed[sj] - smoothed[sj - 1];
    //                if (step >= riseStartDelta)
    //                {
    //                    consecutive++;
    //                    cumulative += step;
    //                    if (consecutive >= minRiseSamples || cumulative >= minCumulativeRise)
    //                    { riseStartIdx = si; break; }
    //                }
    //                else
    //                {
    //                    consecutive = 0; // reset consecutive; keep cumulative
    //                }
    //            }
    //            if (riseStartIdx >= 0) break;
    //        }

    //        if (riseStartIdx < 0) break; // no more rises in data

    //        // ── PHASE 2: Min fuel = Min in 5-min window BEFORE the rise ──────
    //        DateTime riseCutoff = recs[riseStartIdx].Rtc.AddSeconds(-minLookbackSec);
    //        int minIdx = riseStartIdx;
    //        double minFuel = recs[riseStartIdx].FuelLevel;

    //        for (int k = riseStartIdx - 1; k >= 0; k--)
    //        {
    //            if (recs[k].Rtc < riseCutoff) break;
    //            if (recs[k].Speed > maxSpeedDuringRefuel) break;
    //            if (processed.Contains(k)) break;
    //            if (recs[k].FuelLevel < minFuel)
    //            { minFuel = recs[k].FuelLevel; minIdx = k; }
    //        }

    //        // ── PHASE 3: Walk forward through the 15-min event window ────────
    //        var eventWindowEnd = recs[riseStartIdx].Rtc.AddMinutes(timeWindowMinutes);
    //        int j = riseStartIdx + 1;

    //        while (j < n
    //               && !processed.Contains(j)
    //               && recs[j].Speed <= maxSpeedDuringRefuel
    //               && recs[j].Rtc <= eventWindowEnd)
    //        {
    //            double delta = smoothed[j] - smoothed[j - 1];
    //            if (delta < minFuelChangeToBreakEvent) break;
    //            j++;
    //        }
    //        int eventEnd = j - 1;

    //        // ── PHASE 4: Find the STABLE peak in [riseStartIdx .. eventEnd] ──
    //        var candidates = Enumerable.Range(riseStartIdx, eventEnd - riseStartIdx + 1)
    //            .OrderByDescending(idx => smoothed[idx])
    //            .ToList();

    //        int peakIdx = -1;
    //        foreach (int ci in candidates)
    //        {
    //            var pkCutoff = recs[ci].Rtc.AddSeconds(peakStabilityWinSec);
    //            double pkLevel = smoothed[ci];
    //            int confirm = 0;
    //            bool stable = true;

    //            for (int ck = ci + 1; ck < n && recs[ck].Rtc <= pkCutoff; ck++)
    //            {
    //                if (Math.Abs(smoothed[ck] - pkLevel) > peakStabilityBand)
    //                { stable = false; break; }
    //                confirm++;
    //            }
    //            if (stable && confirm >= 2) { peakIdx = ci; break; }
    //        }

    //        // Fallback: highest raw point if no fully-stable candidate found
    //        if (peakIdx < 0)
    //            peakIdx = candidates.Count > 0 ? candidates[0] : riseStartIdx;

    //        double peakFuel = recs[peakIdx].FuelLevel; // always report raw value
    //        double fuelDiff = peakFuel - minFuel;

    //        // ── PHASE 5: Stability flags ──────────────────────────────────────
    //        var preCutoff = recs[minIdx].Rtc.AddSeconds(-stabilityWindowSec);
    //        var preVals = recs.Take(minIdx)
    //                             .Where(r => r.Rtc >= preCutoff)
    //                             .Select(r => r.FuelLevel).ToList();
    //        bool stableBefore = preVals.Count < 2
    //                          || (preVals.Max() - preVals.Min()) <= stabilityBand;

    //        var postCutoff = recs[peakIdx].Rtc.AddSeconds(stabilityWindowSec);
    //        var postVals = recs.Skip(peakIdx)
    //                              .TakeWhile(r => r.Rtc <= postCutoff)
    //                              .Select(r => r.FuelLevel).ToList();
    //        bool stableAfter = postVals.Count < 2
    //                         || (postVals.Max() - postVals.Min()) <= stabilityBand;

    //        string confidence = (stableBefore && stableAfter) ? "High"
    //                          : (stableBefore || stableAfter) ? "Medium"
    //                          : "Low";

    //        TimeSpan duration = recs[peakIdx].Rtc - recs[minIdx].Rtc;

    //        // ── PHASE 6: Tiered validation ────────────────────────────────────
    //        //
    //        //   HIGH   → gain >= 10L  AND duration >= 60s   → ACCEPT
    //        //   MEDIUM → gain >= 25L  AND duration >= 120s  → ACCEPT
    //        //   LOW    → REJECT always (both sides unstable = sensor noise)
    //        //
    //        bool valid = fuelDiff >= minRefuelThreshold
    //                  && fuelDiff <= maxRefuelThreshold
    //                  && confidence != "Low"   // LOW always rejected
    //                  && (
    //                       // HIGH confidence: relaxed size/duration gates
    //                       (confidence == "High"
    //                        && fuelDiff >= highConfMinGain
    //                        && duration.TotalSeconds >= highConfMinDurSec)
    //                       ||
    //                       // MEDIUM confidence: tighter size/duration gates
    //                       (confidence == "Medium"
    //                        && fuelDiff >= medConfMinGain
    //                        && duration.TotalSeconds >= medConfMinDurSec)
    //                     );

    //        // ── DEBUG OUTPUT (remove after tuning) ───────────────────────────
    //        Console.WriteLine(
    //            $"[{(valid ? "ACCEPT" : "REJECT")}][{confidence}] {assetCode} | " +
    //            $"{recs[minIdx].Rtc:HH:mm:ss}→{recs[peakIdx].Rtc:HH:mm:ss} | " +
    //            $"Gain={fuelDiff:F2}L Dur={duration.TotalSeconds:F0}s | " +
    //            $"SB={stableBefore} SA={stableAfter}");
    //        // ─────────────────────────────────────────────────────────────────

    //        if (!valid)
    //        {
    //            for (int k = riseStartIdx; k <= Math.Min(riseStartIdx + 3, n - 1); k++)
    //                processed.Add(k);
    //            i = riseStartIdx + 1;
    //            continue;
    //        }

    //        // ── PHASE 7: Record ───────────────────────────────────────────────
    //        AppendRefuelRow(sb, assetCode, jobCode,
    //                        recs[minIdx], recs[peakIdx],
    //                        minFuel, peakFuel, fuelDiff, confidence);

    //        total += fuelDiff;

    //        for (int k = minIdx; k <= eventEnd && k < n; k++)
    //            processed.Add(k);

    //        i = eventEnd + 1;
    //    }

    //    return (sb.ToString(), total);
    //}





    //static bool IsStableBeforeTime(
    //      List<FuelRecord> recs, int startIdx,
    //      int windowSec, double band)
    //{
    //    if (startIdx == 0) return true;

    //    var cutoff = recs[startIdx].Rtc.AddSeconds(-windowSec);
    //    var vals = recs
    //        .Take(startIdx)
    //        .Where(r => r.Rtc >= cutoff)
    //        .Select(r => r.FuelLevel)
    //        .ToList();

    //    if (vals.Count < 2) return true;
    //    return (vals.Max() - vals.Min()) <= band;
    //}

    //static bool IsStableAfterTime(
    //    List<FuelRecord> recs, int peakIdx,
    //    int windowSec, double band, int n)
    //{
    //    if (peakIdx >= n - 1) return true;

    //    var cutoff = recs[peakIdx].Rtc.AddSeconds(windowSec);
    //    var vals = recs
    //        .Skip(peakIdx)
    //        .TakeWhile(r => r.Rtc <= cutoff)
    //        .Select(r => r.FuelLevel)
    //        .ToList();

    //    if (vals.Count < 2) return true;
    //    return (vals.Max() - vals.Min()) <= band;
    //}


    //static bool IsStableValues(List<double> vals, double band)
    //{
    //    if (vals.Count < 2) return false;
    //    double minVal = vals.Min();
    //    double maxVal = vals.Max();
    //    return (maxVal - minVal) <= band;
    //}


    // --- Helper: emit one HTML table row ---
    //static void AppendRefuelRow(
    //    StringBuilder report,
    //    string assetCode,
    //    string jobCode,
    //    FuelRecord start,
    //    FuelRecord peak,
    //    double startFuel,
    //    double peakFuel,
    //    double fuelDiff,
    //    string confidence)
    //{
    //    // Use background colors to distinguish confidence levels
    //    string rowStyle = confidence switch
    //    {
    //        "Low" => "background-color:#fff3cd",      // Amber
    //        "Medium" => "background-color:#e2f0ff",   // Light blue
    //        _ => ""                                     // High - no background
    //    };

    //    report.AppendLine($"<tr style='{rowStyle}'>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{assetCode}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{jobCode}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{start.Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{peak.Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{startFuel:F2}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{peakFuel:F2}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{fuelDiff:F2}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>Refuel ({confidence})</td>");
    //    report.AppendLine("</tr>");
    //}






    //static (string report, double totalRefueled) AnalyzeFuelData(List<FuelRecord> records, string jobCode, string assetCode)
    //{
    //    StringBuilder report = new();
    //    double refuelTotal = 0;

    //    // ── Tunable thresholds ────────────────────────────────────────────────
    //    const double minRefuelThreshold = 10.0;      // min fuel gain (L)
    //    const double maxRefuelThreshold = 1200.0;    // sanity cap (L)
    //    const int timeWindowMinutes = 15;            // fixed window size (minutes)
    //    const double maxSpeedDuringRefuel = 0.0;     // vehicle must be stationary
    //    const double stabilityBand = 2.0;            // allowed fuel swing (L)
    //    const int stabilityWindowSec = 60;           // pre/post stability window (s)
    //    const int minRefuelDurationSeconds = 120;     // minimum fill duration (s)
    //    // ──────────────────────────────────────────────────────────────────────

    //    if (records == null || records.Count == 0)
    //        return (string.Empty, refuelTotal);

    //    // Step 1: Clean and sort records
    //    var recs = records
    //        .Where(r => r.FuelLevel > 0 && r.Rtc != DateTime.MinValue)
    //        .OrderBy(r => r.Rtc)
    //        .ToList();

    //    if (recs.Count < 2)
    //        return (string.Empty, refuelTotal);

    //    int n = recs.Count;
    //    DateTime minTime = recs[0].Rtc;
    //    DateTime maxTime = recs[n - 1].Rtc;

    //    // Step 2: Create fixed time windows (e.g., 12:00-12:15, 12:15-12:30, etc.)
    //    var windows = new List<(DateTime start, DateTime end)>();
    //    DateTime currentWindowStart = new DateTime(minTime.Year, minTime.Month, minTime.Day, minTime.Hour,
    //                                       (minTime.Minute / timeWindowMinutes) * timeWindowMinutes, 0);

    //    while (currentWindowStart <= maxTime)
    //    {
    //        DateTime windowEnd = currentWindowStart.AddMinutes(timeWindowMinutes);
    //        windows.Add((currentWindowStart, windowEnd));
    //        currentWindowStart = windowEnd;
    //    }

    //    // Step 3: Process each window independently
    //    foreach (var (winStart, winEnd) in windows)
    //    {
    //        // Get records in this window
    //        var windowRecs = recs
    //            .Where(r => r.Rtc >= winStart && r.Rtc < winEnd)
    //            .ToList();

    //        if (windowRecs.Count < 2)
    //            continue;

    //        // Process refuels within this window
    //        ProcessWindowRefuels(windowRecs, recs, winStart, winEnd,
    //                            ref report, ref refuelTotal, assetCode, jobCode,
    //                            minRefuelThreshold, maxRefuelThreshold,
    //                            maxSpeedDuringRefuel, stabilityBand, stabilityWindowSec,
    //                            minRefuelDurationSeconds);
    //    }

    //    return (report.ToString(), refuelTotal);
    //}

    ///// <summary>
    ///// Processes all refuel events within a single 15-minute window
    ///// </summary>
    //static void ProcessWindowRefuels(
    //    List<FuelRecord> windowRecs,
    //    List<FuelRecord> allRecs,
    //    DateTime windowStart,
    //    DateTime windowEnd,
    //    ref StringBuilder report,
    //    ref double refuelTotal,
    //    string assetCode,
    //    string jobCode,
    //    double minRefuelThreshold,
    //    double maxRefuelThreshold,
    //    double maxSpeedDuringRefuel,
    //    double stabilityBand,
    //    int stabilityWindowSec,
    //    int minRefuelDurationSeconds)
    //{
    //    int winCount = windowRecs.Count;
    //    var processed = new HashSet<int>();

    //    int i = 0;
    //    while (i < winCount)
    //    {
    //        // Skip if already processed or vehicle is moving
    //        if (processed.Contains(i) || windowRecs[i].Speed > maxSpeedDuringRefuel)
    //        {
    //            i++;
    //            continue;
    //        }

    //        // ═══ PHASE 1: Find the minimum fuel (start point) ═══
    //        int startIdx = i;
    //        double startFuel = windowRecs[i].FuelLevel;

    //        // Look backward within window for lower fuel (within 5 min)
    //        int lookbackIdx = i;
    //        while (lookbackIdx > 0)
    //        {
    //            double timeSec = (windowRecs[lookbackIdx].Rtc - windowRecs[lookbackIdx - 1].Rtc).TotalSeconds;

    //            if (timeSec > 300) // 5 minute threshold
    //                break;

    //            if (windowRecs[lookbackIdx - 1].Speed > maxSpeedDuringRefuel)
    //                break;

    //            if (windowRecs[lookbackIdx - 1].FuelLevel < startFuel)
    //            {
    //                lookbackIdx--;
    //                startIdx = lookbackIdx;
    //                startFuel = windowRecs[lookbackIdx].FuelLevel;
    //            }
    //            else
    //            {
    //                break; // Found minimum
    //            }
    //        }

    //        // ═══ PHASE 2: Walk forward to find the peak (end point) ═══
    //        int peakIdx = startIdx;
    //        double peakFuel = startFuel;
    //        bool foundRise = false;

    //        int j = startIdx + 1;
    //        while (j < winCount)
    //        {
    //            // Stop if vehicle moves
    //            if (windowRecs[j].Speed > maxSpeedDuringRefuel)
    //                break;

    //            double fuelChange = windowRecs[j].FuelLevel - windowRecs[j - 1].FuelLevel;

    //            // Track if we found any meaningful rise
    //            if (fuelChange > 0.2)
    //                foundRise = true;

    //            // Update peak if we find higher fuel
    //            if (windowRecs[j].FuelLevel > peakFuel)
    //            {
    //                peakFuel = windowRecs[j].FuelLevel;
    //                peakIdx = j;
    //            }

    //            // Stop on significant decrease (actual fuel consumption)
    //            if (foundRise && fuelChange < -0.2 && windowRecs[j].FuelLevel == windowRecs[j-1].FuelLevel)
    //                break;

    //            j++;
    //        }

    //        // If no rise found, skip this point
    //        if (!foundRise || peakIdx == startIdx)
    //        {
    //            i++;
    //            continue;
    //        }

    //        // ═══ PHASE 3: Validate the refuel event ═══
    //        double fuelDiff = peakFuel - startFuel;

    //        // Gate 1: Check fuel gain thresholds
    //        if (fuelDiff < minRefuelThreshold || fuelDiff > maxRefuelThreshold)
    //        {
    //            i++;
    //            continue;
    //        }

    //        // Gate 2: Check duration
    //        TimeSpan duration = windowRecs[peakIdx].Rtc - windowRecs[startIdx].Rtc;
    //        if (duration.TotalSeconds < minRefuelDurationSeconds)
    //        {
    //            i++;
    //            continue;
    //        }

    //        // ═══ PHASE 4: Check stability before and after ═══
    //        // Find indices in ALL records for stability check
    //        int globalStartIdx = allRecs.FindIndex(r => r.Rtc == windowRecs[startIdx].Rtc &&
    //                                                     r.FuelLevel == windowRecs[startIdx].FuelLevel && r.DeviceId == windowRecs[startIdx].DeviceId);
    //        int globalPeakIdx = allRecs.FindIndex(r => r.Rtc == windowRecs[peakIdx].Rtc &&
    //                                                    r.FuelLevel == windowRecs[peakIdx].FuelLevel && r.DeviceId== windowRecs[peakIdx].DeviceId);

    //        if (globalStartIdx<0)
    //        {
    //            i++;
    //            continue;
    //        }
    //        if (globalPeakIdx<0)
    //        {
    //            i++;
    //            continue;
    //        }

    //        bool stableBefore = IsStableBeforeTime(allRecs, globalStartIdx, stabilityWindowSec, stabilityBand);
    //        bool stableAfter = IsStableAfterTime(allRecs, globalPeakIdx, stabilityWindowSec, stabilityBand, allRecs.Count);

    //        // ═══ PHASE 5: Determine confidence ═══
    //        string confidence = "Low";
    //        if (stableBefore && stableAfter)
    //            confidence = "High";
    //        else if (stableBefore || stableAfter)
    //            confidence = "Medium";

    //        // ═══ PHASE 6: Log and record ═══
    //        Console.WriteLine(
    //            $"[{confidence.ToUpper()}] Asset={assetCode} | " +
    //            $"Window=[{windowStart:HH:mm:ss}-{windowEnd:HH:mm:ss}] | " +
    //            $"{windowRecs[startIdx].Rtc:HH:mm:ss} → {windowRecs[peakIdx].Rtc:HH:mm:ss} | " +
    //            $"Fuel: {startFuel:F2}L → {peakFuel:F2}L | Gain={fuelDiff:F2}L | Dur={duration.TotalSeconds:F0}s");

    //        AppendRefuelRow(report, assetCode, jobCode,
    //                       windowRecs[startIdx], windowRecs[peakIdx],
    //                       startFuel, peakFuel, fuelDiff, confidence);

    //        refuelTotal += fuelDiff;

    //        // Mark as processed
    //        for (int k = startIdx; k <= peakIdx && k < winCount; k++)
    //            processed.Add(k);

    //        i = peakIdx + 1;
    //    }
    //}

    //static bool IsStableBeforeTime(
    //    List<FuelRecord> recs, int startIdx,
    //    int windowSec, double band)
    //{
    //    if (startIdx <= 0) return true;

    //    var cutoff = recs[startIdx].Rtc.AddSeconds(-windowSec);
    //    var vals = recs
    //        .Take(startIdx)
    //        .Where(r => r.Rtc >= cutoff)
    //        .Select(r => r.FuelLevel)
    //        .ToList();

    //    if (vals.Count < 2) return true;
    //    return (vals.Max() - vals.Min()) <= band;
    //}

    //static bool IsStableAfterTime(
    //    List<FuelRecord> recs, int peakIdx,
    //    int windowSec, double band, int n)
    //{
    //    if (peakIdx >= n - 1) return true;

    //    var cutoff = recs[peakIdx].Rtc.AddSeconds(windowSec);
    //    var vals = recs
    //        .Skip(peakIdx)
    //        .TakeWhile(r => r.Rtc <= cutoff)
    //        .Select(r => r.FuelLevel)
    //        .ToList();

    //    if (vals.Count < 2) return true;
    //    return (vals.Max() - vals.Min()) <= band;
    //}

    //static void AppendRefuelRow(
    //    StringBuilder report,
    //    string assetCode,
    //    string jobCode,
    //    FuelRecord start,
    //    FuelRecord peak,
    //    double startFuel,
    //    double peakFuel,
    //    double fuelDiff,
    //    string confidence)
    //{
    //    string rowStyle = confidence switch
    //    {
    //        "Low" => "background-color:#fff3cd",
    //        "Medium" => "background-color:#e2f0ff",
    //        _ => ""
    //    };

    //    report.AppendLine($"<tr style='{rowStyle}'>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{assetCode}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{jobCode}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{start.Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{peak.Rtc:yyyy-MM-dd HH:mm:ss}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{startFuel:F2}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{peakFuel:F2}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>{fuelDiff:F2}</td>");
    //    report.AppendLine($"<td style='border:1px solid #ddd;padding:8px'>Refuel ({confidence})</td>");
    //    report.AppendLine("</tr>");
    //}



}

