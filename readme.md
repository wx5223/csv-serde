# Hive CSV Support

[![Build Status](https://drone.io/github.com/ogrodnek/csv-serde/status.png)](https://drone.io/github.com/ogrodnek/csv-serde/latest)

This SerDe adds *real* CSV input and ouput support to hive using the excellent [opencsv](http://opencsv.sourceforge.net/) library.

## Using


### Basic Use

```
add jar path/to/csv-serde.jar;

create table my_table(a string, b string, ...)
  row format serde 'com.bizo.hive.serde.csv.CSVSerde'
  stored as textfile
;
```

### Custom formatting

The default separator, quote, and escape characters from the `opencsv` library are:

```
DEFAULT_ESCAPE_CHARACTER \
DEFAULT_QUOTE_CHARACTER  "
DEFAULT_SEPARATOR        ,
```

You can also specify custom separator, quote, or escape characters.

```
add jar path/to/csv-serde.jar;

create table my_table(a string, b string, ...)
 row format serde 'com.bizo.hive.serde.csv.CSVSerde'
 with serdeproperties (
   "separatorChar" = "\t",
   "quoteChar"     = "'",
   "escapeChar"    = "\\"
  )	  
 stored as textfile
;
```

### CSVTextInputFormat
CSVTextInputFormat used for default csv like mongoexport csv file, in order to handle normal \ char 
use \005 instead of \ for escape
#### test csv file
```$xslt
"a\bc\""","def\nabc","abc,d""dd"
```
#### result 
```$xslt
a\bc\"  def\nabc    abc,d"dd
```
#### hql
```
CREATE TABLE mytable(a string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( "separatorChar" = ',', "quoteChar" = '"', "escapeChar" = '\005' )
STORED AS INPUTFORMAT 'com.shawn.hive.serde.CSVTextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
```

## Files

todo

## Building

Run `mvn package` to build.  Both a basic artifact as well as a "fat jar" (with opencsv) are produced.

### Eclipse support

Run `mvn eclipse:eclipse` to generate `.project` and `.classpath` files for eclipse.


## License

csv-serde is open source and licensed under the [Apache 2 License](http://www.apache.org/licenses/LICENSE-2.0.html).
