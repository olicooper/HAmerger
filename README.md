# Home Assistant Database Merge Tool

A cross-platform tool to merge the statistic tables in the database created by Home Assistant. This tool should be able to support `Sqlite`, `MySql` and `SqlServer` (although I've only tested Sqlite). Built on .NET7 (cross-platform compatible), using OrmLite to manipulate the databases.

If you want to get started quickly, simply:

* Open the project in Visual Studio 2022 
* Build the project (`Ctrl + Shift + B`)
* Locate the build folder within the project folder `bin/`, create a folder called `data` and place your old and new database files in the folder (naming them `old.db` and `new.db`).
* Run the program and wait for the data to be migrated.

## Warning

This tool was only tested on my personal HA instance (schema version 35 and 41), it may not work for you!

* Only work on COPIES of your databases to ensure no data loss.
* If you need to re-run this script I suggest you re-copy the databases on each run to ensure a consistent state (though you can also set `SKIP_DUPLICATE_CHECK=false`).

## Notes

* There are a few options that can be set prior to building the project:
    * `DB_TYPE` - Select your database type here. Options are Sqlite, MySql, SqlServer. Default is Sqlite
    * `OLD_DB_CONNECTION_STRING` - Set the OLD database string here. Default is 'old.db'
    * `NEW_DB_CONNECTION_STRING` - Set the NEW database string here. Default is 'new.db'
    * `DRY_RUN` - Setting this to 'true' will ensure the new database is not modified, so you can see if/how it works. By default this is `true` for safety reasons.
    * `SKIP_DUPLICATE_CHECK` - Setting this to 'true' will speed up the time it takes to merge the databases but may lead to inconsistent data. Default is `true` to speed things up.
    * `COPY_TO_NEW_DB` - Setting this to 'true' will create a mirror copy of the statistic tables in the new database after all records have been processed. This allows you to use your new database whilst keeping all the old statistics. Default is `false`.
* This tool currently doesn't copy the `state` tables. Tables copied are `statistics`, `statistics_short_term`, `statistics_meta`, `statistics_runs`.