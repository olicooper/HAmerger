using Humanizer;
using ServiceStack;
using ServiceStack.DataAnnotations;
using ServiceStack.OrmLite;
using ServiceStack.Text;
using System.Diagnostics;
using System.Transactions;

// WARNING: This is only tested on my data, it may not work for you.
//          Only work on COPIES of your databases to ensure no data loss.
//          If you need to re-run this script I suggest you re-copy the databases on each run to ensure a consistent state.

// NOTE: If you are using Sqlite, place your database files under a folder called 'data' with the names
//       'old.db' and 'new.db' (you can change the names below if you want),
//       otherwise update your database type and connection strings to suit your setup.
//       Other modifiable options are: DRY_RUN, SKIP_DUPLICATE_CHECK and COPY_TO_NEW_DB.

static IOrmLiteDialectProvider GetProvider(DatabaseType type) => type switch 
{
    DatabaseType.Sqlite => SqliteDialect.Provider,
    DatabaseType.MySql => MySqlDialect.Provider,
    DatabaseType.SqlServer => SqlServerDialect.Provider,
    _ => throw new ArgumentException("Invalid database type"),
};

// Select your database type here. Options are Sqlite, MySql, SqlServer
// Be sure to update the connection strings below too!
DatabaseType DB_TYPE = DatabaseType.Sqlite;
string OLD_DB_CONNECTION_STRING = Path.Combine("data", "old.db");
string NEW_DB_CONNECTION_STRING = Path.Combine("data", "new.db");

// Setting this to 'true' will ensure the new database is not modified, so you can see if/how it works.
// Tip: You can see all the modified data in the file 'temp.db'
bool DRY_RUN = true; // args.Any(x => x == "--dry-run");

// Setting this to 'true' will speed up the time it takes to merge the databases but may lead to inconsistent data
bool SKIP_DUPLICATE_CHECK = true; // args.Any(x => x == "--skip-duplicate-check");

// Setting this to 'true' will create a mirror copy of the statistic tables in the
// new database after all records have been processed.
// This allows you to use your new database whilst keeping all the old statistics.
bool COPY_TO_NEW_DB = false; // args.Any(x => x == "--copy-to-new-db");


Console.WriteLine($"Starting data merge{(DRY_RUN ? " (dry run)" : string.Empty)}");
var stopwatch = Stopwatch.StartNew();

var dbFactory = new OrmLiteConnectionFactory(OLD_DB_CONNECTION_STRING, GetProvider(DB_TYPE));
dbFactory.RegisterConnection("OldDb", OLD_DB_CONNECTION_STRING, GetProvider(DB_TYPE));
dbFactory.RegisterConnection("NewDb", NEW_DB_CONNECTION_STRING, GetProvider(DB_TYPE));
dbFactory.RegisterConnection("TempDb", Path.Combine("data", "temp.db"), GetProvider(DatabaseType.Sqlite));

using (var oldDb = dbFactory.OpenDbConnection("OldDb"))
using (var newDb = dbFactory.OpenDbConnection("NewDb"))
using (var tempDb = dbFactory.OpenDbConnection("TempDb"))
{
    var metadataMatches = new List<MetaMatch>();

    // 1. Match metadata records between the old and new database
    var oldMetaRows = oldDb.Select<StatisticMeta>();
    var newMetaRows = newDb.Select<StatisticMeta>();

    if (oldMetaRows.Count == 0)
    {
        Console.WriteLine($"\nWARN: No statistic metadata in the old database!");
        return;
    }

    int statisticIdHeaderLength = oldMetaRows.Max(x => x.StatisticId.Length) + 2;
    var matchesConsoleString = string.Empty;
    foreach (var oldRow in oldMetaRows)
    {
        var newRow = newMetaRows.FirstOrDefault(x => x.StatisticId == oldRow.StatisticId);
        metadataMatches.Add(new MetaMatch
        {
            OldId = oldRow.Id,
            NewId = newRow?.Id,
            EntityId = oldRow.StatisticId,
            HasSum = oldRow.HasSum
        });

        matchesConsoleString += string.Format(
            $"|{{0,8}}|{{1,8}}|{{2,{statisticIdHeaderLength}}}|{{3,14}}|\n",
            oldRow.Id,
            newRow?.Id.ToString() ?? "NONE",
            oldRow.StatisticId,
            newRow != null && oldRow.HasSum ? "YES      " : "NO      ");
    }

    Console.WriteLine($"\nStatistic metadata matches:");
    var headerStr = string.Format($"|{{0,8}}|{{1,8}}|{{2,{statisticIdHeaderLength}}}|{{3,14}}|", "old id", "new id", "statistic_id", "recalculate?");
    Console.WriteLine(headerStr);
    Console.WriteLine(new string('-', headerStr.Length));
    Console.WriteLine(matchesConsoleString + "\n");
    // 1. END

    var metaMatchesToRemap = metadataMatches.Where(x => x.NewId.HasValue).ToList();
    if (metaMatchesToRemap.Count > 0)
    {
        // 2. Populate the old 'statistics' table with data from the new database and recalculate the 'sum' column if required
        Console.WriteLine($"Processing 'statistics' table...\n");

        tempDb.DropAndCreateTable<Statistic>();

        List<Statistic> newStatisticRows = new();
        int duplicateStatisticRowCount = 0;
        foreach (var metaMatch in metaMatchesToRemap)
        {
            Statistic? oldDbLastRecordedStatistic = null;
            var newStatisticQuery = newDb.From<Statistic>()
                .Where(x => x.MetadataId == metaMatch.NewId);

            if (metaMatch.HasSum)
            {
                oldDbLastRecordedStatistic = oldDb.Select(oldDb.From<Statistic>()
                    .Where(x => x.MetadataId == metaMatch.OldId)
                    .OrderByDescending(x => x.Id).Limit(1))
                    .FirstOrDefault();
                if (oldDbLastRecordedStatistic == null) continue;

                // Ensure only new data is added which doesnt conflict with the data
                if (oldDbLastRecordedStatistic.CreatedTs.HasValue)
                {
                    var lastDate = DateTimeExtensions.FromUnixTime(oldDbLastRecordedStatistic.CreatedTs.Value);
                    newStatisticQuery = newStatisticQuery
                        .Where(x => x.CreatedTs > oldDbLastRecordedStatistic.CreatedTs || x.Created > lastDate);
                }
                else if (oldDbLastRecordedStatistic.Created.HasValue)
                {
                    var lastDate = Convert.ToDouble(oldDbLastRecordedStatistic.Created.Value.ToUnixTimeMs());
                    newStatisticQuery = newStatisticQuery
                        .Where(newStatistic => newStatistic.Created > oldDbLastRecordedStatistic.Created || newStatistic.CreatedTs > lastDate);
                }
                else
                {
                    Console.WriteLine($"WARN: Cannot determine last recorded statistic for entity '{metaMatch.EntityId}', skipping insert!");
                    continue;
                }
            }

            var tempStatisticRows = newDb.Select(newStatisticQuery).ToList();

            if (tempStatisticRows.Count > 0)
            {
                foreach (var row in tempStatisticRows)
                {
                    // Skip duplicate records
                    // todo: how do we check potentially millions of records for a match efficiently?
                    if (!SKIP_DUPLICATE_CHECK)
                    {
                        if (row.StartTs.HasValue)
                        {
                            //if (oldStatisticRows.Any(x => x.StartTs == row.StartTs))
                            if (oldDb.Count<Statistic>(x => x.MetadataId == metaMatch.OldId && x.StartTs == row.StartTs) > 0)
                            {
                                duplicateStatisticRowCount++;
                                continue;
                            }
                        }
                        else if (row.Start.HasValue)
                        {
                            //if (oldStatisticRows.Any(x => x.Start == row.Start))
                            if (oldDb.Count<Statistic>(x => x.MetadataId == metaMatch.OldId && x.Start == row.Start) > 0)
                            {
                                duplicateStatisticRowCount++;
                                continue;
                            }
                        }
                        else
                        {
                            continue;
                        }
                    }

                    row.MetadataId = metaMatch.OldId;

                    if (oldDbLastRecordedStatistic != null)
                        row.Sum += oldDbLastRecordedStatistic.Sum;
                
                    newStatisticRows.Add(row);
                }

                Console.WriteLine($"{newStatisticRows.Count}{(metaMatch.HasSum ? " recalculated" : string.Empty)} rows to insert for entity '{metaMatch.EntityId}'");
            }
        }

        if (newStatisticRows.Count > 0)
        {
            newStatisticRows = newStatisticRows.OrderBy(x => x.Id).ToList();

            if (!DRY_RUN) oldDb.InsertAll(newStatisticRows);
            tempDb.InsertAll(newStatisticRows);

            Console.WriteLine($"\n{newStatisticRows.Count} rows inserted in 'statistics' table");
        }

        if (duplicateStatisticRowCount > 0)
        {
            Console.WriteLine($"{duplicateStatisticRowCount} duplicate rows found in 'statistics' table");
        }
        Console.WriteLine($"Elapsed time (so far) {stopwatch.Elapsed.Humanize()}\n");
        // 2. END

        // 3. Populate the old 'statistics_short_term' table with data from the new database and recalculate the 'sum' column if required
        Console.WriteLine($"\nProcessing 'statistics_short_term' table...\n");

        tempDb.DropAndCreateTable<StatisticShortTerm>();

        List<StatisticShortTerm> newStatisticShortTermRows = new();
        int duplicateStatisticShortTermRowCount = 0;
        foreach (var metaMatch in metaMatchesToRemap)
        {
            StatisticShortTerm? oldDbLastRecordedStatistic = null;
            var newStatisticQuery = newDb.From<StatisticShortTerm>()
                .Where(x => x.MetadataId == metaMatch.NewId);

            if (metaMatch.HasSum)
            {
                oldDbLastRecordedStatistic = oldDb.Select(oldDb.From<StatisticShortTerm>()
                    .Where(x => x.MetadataId == metaMatch.OldId)
                    .OrderByDescending(x => x.Id).Limit(1))
                    .FirstOrDefault();
                if (oldDbLastRecordedStatistic == null) continue;

                // Ensure only new data is added which doesnt conflict with the data
                if (oldDbLastRecordedStatistic.CreatedTs.HasValue)
                {
                    var lastDate = DateTimeExtensions.FromUnixTime(oldDbLastRecordedStatistic.CreatedTs.Value);
                    newStatisticQuery = newStatisticQuery
                        .Where(x => x.CreatedTs > oldDbLastRecordedStatistic.CreatedTs || x.Created > lastDate);
                }
                else if (oldDbLastRecordedStatistic.Created.HasValue)
                {
                    var lastDate = Convert.ToDouble(oldDbLastRecordedStatistic.Created.Value.ToUnixTimeMs());
                    newStatisticQuery = newStatisticQuery
                        .Where(newStatistic => newStatistic.Created > oldDbLastRecordedStatistic.Created || newStatistic.CreatedTs > lastDate);
                }
                else
                {
                    Console.WriteLine($"WARN: Cannot determine last recorded statistic for entity '{metaMatch.EntityId}', skipping insert!");
                    continue;
                }
            }

            var tempStatisticRows = newDb.Select(newStatisticQuery).ToList();

            if (tempStatisticRows.Count > 0)
            {
                foreach (var row in tempStatisticRows)
                {
                    // Skip duplicate records
                    if (!SKIP_DUPLICATE_CHECK)
                    {
                        if (row.StartTs.HasValue)
                        {
                            if (oldDb.Count<StatisticShortTerm>(x => x.MetadataId == metaMatch.OldId && x.StartTs == row.StartTs) > 0)
                            {
                                duplicateStatisticRowCount++;
                                continue;
                            }
                        }
                        else if (row.Start.HasValue)
                        {
                            if (oldDb.Count<StatisticShortTerm>(x => x.MetadataId == metaMatch.OldId && x.Start == row.Start) > 0)
                            {
                                duplicateStatisticRowCount++;
                                continue;
                            }
                        }
                        else
                        {
                            continue;
                        }
                    }

                    row.MetadataId = metaMatch.OldId;

                    if (oldDbLastRecordedStatistic != null)
                        row.Sum += oldDbLastRecordedStatistic.Sum;
                
                    newStatisticShortTermRows.Add(row);
                }

                Console.WriteLine($"{newStatisticShortTermRows.Count}{(metaMatch.HasSum ? " recalculated" : string.Empty)} rows to insert for entity '{metaMatch.EntityId}'");
            }
        }

        if (newStatisticShortTermRows.Count > 0)
        {
            newStatisticShortTermRows = newStatisticShortTermRows.OrderBy(x => x.Id).ToList();

            if (!DRY_RUN) oldDb.InsertAll(newStatisticShortTermRows);
            tempDb.InsertAll(newStatisticShortTermRows);

            Console.WriteLine($"\n{newStatisticShortTermRows.Count} rows inserted in 'statistics_short_term' table");
        }

        if (duplicateStatisticShortTermRowCount > 0)
        {
            Console.WriteLine($"{duplicateStatisticShortTermRowCount} duplicate rows found in 'statistics_short_term' table");
        }
        Console.WriteLine($"Elapsed time (so far) {stopwatch.Elapsed.Humanize()}\n");
        // 3. END
    }
    else
    {
        Console.WriteLine($"\nWARN: No statistic metadata matches found\n");
    }

    // 4. Copy newer 'statistic_runs' entries from the new database to the old database
    var lastStatisticRun = oldDb.Select(oldDb.From<StatisticRun>()
        .OrderByDescending(x => x.Start).Limit(1))
        .FirstOrDefault();

    var statisticRunQuery = newDb.From<StatisticRun>();
    if (lastStatisticRun != null)
    {
        statisticRunQuery = statisticRunQuery.Where(x => x.Start > lastStatisticRun.Start);
    }

    var statisticRunsToCopy = newDb.Select(statisticRunQuery);
    if (statisticRunsToCopy.Count > 0)
    {
        tempDb.DropAndCreateTable<StatisticRun>();
        if (!DRY_RUN) oldDb.InsertAll(statisticRunsToCopy);
        tempDb.InsertAll(statisticRunsToCopy);
        Console.WriteLine($"\nCopied {statisticRunsToCopy.Count} rows in to 'statistics_runs' table (elapsed time so far {stopwatch.Elapsed.Humanize()})\n");
    }
    // 4. END

    // 5. TODO: (optionally) Copy the remaining/new data (that hasn't been recalculated and inserted already) from the new db to the old
    // 5. END

    // 6. (optionally) Copy data back to the new database, syncronizing both databases.
    if (COPY_TO_NEW_DB)
    {
        Console.WriteLine($"\nCopying statistic data from the old database to the new database...\n");
        using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew, TimeSpan.FromMinutes(3)))
        {
            if (!DRY_RUN) newDb.DeleteAll<StatisticMeta>();
            Console.WriteLine($"[NewDB] Deleted all 'statistics_meta' rows");
            if (!DRY_RUN) newDb.DeleteAll<Statistic>();
            Console.WriteLine($"[NewDB] Deleted all 'statistics' rows");
            if (!DRY_RUN) newDb.DeleteAll<StatisticShortTerm>();
            Console.WriteLine($"[NewDB] Deleted all 'statistics_short_term' rows");
            if (!DRY_RUN) newDb.DeleteAll<StatisticRun>();
            Console.WriteLine($"[NewDB] Deleted all 'statistics_runs' rows");

            var allStatisticMetaRows = oldDb.Select<StatisticMeta>();
            var allStatisticRows = oldDb.Select<Statistic>();
            var allStatisticShortTermRows = oldDb.Select<StatisticShortTerm>();
            var allStatisticRunRows = oldDb.Select<StatisticRun>();

            if (!DRY_RUN) newDb.InsertAll(allStatisticMetaRows);
            Console.WriteLine($"[NewDB] Inserted {allStatisticMetaRows.Count} rows in to 'statistics_meta' table");
            if (!DRY_RUN) newDb.InsertAll(allStatisticRows);
            Console.WriteLine($"[NewDB] Inserted {allStatisticRows.Count} rows in to 'statistics' table");
            if (!DRY_RUN) newDb.InsertAll(allStatisticShortTermRows);
            Console.WriteLine($"[NewDB] Inserted {allStatisticShortTermRows.Count} rows in to 'statistics_short_term' table");
            if (!DRY_RUN) newDb.InsertAll(allStatisticRunRows);
            Console.WriteLine($"[NewDB] Inserted {allStatisticRunRows.Count} rows in to 'statistics_runs' table (elapsed time so far {stopwatch.Elapsed.Humanize()})");

            scope.Complete();
        }
        stopwatch.Stop();
        Console.WriteLine($"\nFinished copying statistic data in {stopwatch.Elapsed.Humanize()}!\n");
    }
    // 6. END
}

public struct MetaMatch
{
    public int OldId;
    public int? NewId;
    public string EntityId;
    public bool HasSum;
}

public enum DatabaseType
{
    Sqlite,
    MySql,
    SqlServer
}

[Alias("statistics_runs")]
public class StatisticRun
{
    [PrimaryKey]
    [AutoIncrement]
    [Alias("id")] public int Id { get; set; }
    [Alias("start")] public DateTime Start { get; set; }
}

[Alias("statistics_meta")]
public class StatisticMeta
{
    [PrimaryKey]
    [AutoIncrement]
    [Alias("id")] public int Id { get; set; }
    [Alias("statistic_id")] public string StatisticId { get; set; } = "";
    [Alias("source")] public string Source { get; set; } = "";
    [Alias("unit_of_measurement")] public string UnitOfMeasurement { get; set; } = "";
    [Alias("name")] public string Name { get; set; } = "";
    [Alias("has_mean")] public bool HasMean { get; set; }
    [Alias("has_sum")] public bool HasSum { get; set; }
}

[Alias("statistics")]
public class Statistic
{
    [PrimaryKey]
    [AutoIncrement]
    [Alias("id")] public int Id { get; set; }
    [Alias("metadata_id")] public int MetadataId { get; set; }
    [Alias("created")] public DateTime? Created { get; set; }
    [Alias("created_ts")] public double? CreatedTs { get; set; }
    [Alias("start")] public DateTime? Start { get; set; }
    [Alias("start_ts")] public double? StartTs { get; set; }
    [Alias("mean")] public double? Mean { get; set; }
    [Alias("min")] public double? Min { get; set; }
    [Alias("max")] public double? Max { get; set; } 
    [Alias("last_reset")] public DateTime? LastReset { get; set; }
    [Alias("last_reset_ts")] public double? LastResetTs { get; set; }
    [Alias("state")] public double? State { get; set; }
    [Alias("sum")] public double? Sum { get; set; }
}

[Alias("statistics_short_term")]
public class StatisticShortTerm : Statistic { }
