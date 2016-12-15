# CSV vs. Parquet
This is an attempt to see if CSV files are slower than Parquet files for Visibility Distribution. It's a demo. 

Ping me for the csv file. 

## Schema File
The Schema file looks like this:
```
message row {
  required int32 time_period_id;
  required int32 tracked_search_id;
  optional int32 rank;
}
```
