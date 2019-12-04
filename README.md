# Data generator

## Overview
Generates artificial ('fake') data from provided schema.
Produces a Spark dataframe or writes to files.

## Features
- Support for basic as well as complex data types (e.g. structs, arrays)
- Support for several content types for StringType fields
- Ability to save generated data into files with names following
  specified pattern
- Can be used locally or in Databricks

## Usage

```python
from pyspark.sql.types import *
from datagen import DataGenerator

schema = StructType([
    StructField("id", ByteType(), False),
    StructField("name", StringType(), True, metadata={"content_type": "phrase", "length": 2}),
    StructField("price", DecimalType(5, 2), True),
    StructField("dt", DateType(), False),
    StructField("cats", ArrayType(IntegerType(), False), True, metadata={"length": 3, "metadata": {"length": 5}}),
])

df = DataGenerator(spark).generate_df(schema)
df.show(5, truncate=False)
""" Output:
+---+----------------+------+----------+---------------------+
|id |name            |price |dt        |cats                 |
+---+----------------+------+----------+---------------------+
|80 |thank communist |362.08|2019-10-02|[51131, 53087, 52115]|
|39 |hire submission |null  |2019-10-23|[81864, 68371, 77252]|
|78 |per exile       |null  |2019-06-27|[49452, 98832, 58133]|
|7  |stupid clock    |293.04|2018-12-30|[27517, 57206, 38612]|
|76 |peer colourful  |742.35|2019-01-01|[23231, 59085, 81540]|
+---+----------------+------+----------+---------------------+
"""

DataGenerator(spark).generate\
    .options(num_files=5, num_records=5000, file_format="json")\
    .options(file_name_pattern="test_json_data_[yyyyMMdd].json")\
    .option("date_increment", "2 days")\
    .schema(schema)\
    .save_to("/path/to/data/dir/")

import os
os.listdir("/path/to/data/dir/")
""" Output:
['test_json_data_20191104.json',
 'test_json_data_20191105.json',
 'test_json_data_20191030.json',
 'test_json_data_20191103.json',
 'test_json_data_20191028.json',
 'test_json_data_20191101.json']
"""
```

## Data types

Data generator will generate random values based on the data type of a specific schema field.
For some data types, however, there are additional parameters that can be specified in `StructField`'s
metadata property that will impact the value produced. If the field has nullable property set to
`True` generated values will be randomly set to `None`.

### BooleanType()
* Possible values: `True` or `False`

### ByteType()
* Possible values: integer numbers in the range `[-99, 99]`
* Metadata params:
  - `length`: number of digits in the generated number. An integer (e.g. `1` or `2`) or tuple
  (`(1, 2)`) with min and max length boundaries. Default: `(1-2)`.
  - `allow_negative`: determines if negative values are allowed. Default: `False`.
* Example:
  ```python
  StructField("some_field", ByteType(), False, metadata={"length": 1, "allow_negative": True})
  ```

### IntegerType() / LongType()
* Possible values: integer numbers in the range `[-999,999,999, 999,999,999]` (1-9 digits) for 
`IntegerType` and integer numbers in the range `[-999,999,999,999,999,999, 999,999,999,999,999,999]`
(1-18 digits) for `LongType`.
* Metadata params:
  - `length`: number of digits in the generated number. An integer (`1-9`) for `IntegerType` or
  (`1-18`) for `LongType` or tuple. Default: `(1, 9)` or `(1, 18)`.
  - `allow_negative`: determines if negative values are allowed. Default: `False`.
* Example:
  ```python
  StructField("some_field", IntegerType(), False, metadata={"length": (3, 5), "allow_negative": True})
  ```

### FloatType() / DoubleType()
* Possible values: floating point numbers in the range `[-999,999,999.0, 999,999,999.0]`
(1-9 non-decimal digits) for `FloatType` or floating point numbers in the range
`[-999,999,999,999,999,999.0, 999,999,999,999,999,999.0]` (1-18 non-decimal digits) for `DoubleType`.
* Metadata params:
  - `length`: number of non-decimal digits in the generated number. An integer (`1-9`) for
  `FloatType` or (`1-18`) for `DoubleType` or tuple with min and max boundaries. Default: `(1, 9)`
  or `(1, 18)`.
  - `decimal_precision`: rounding precision / number of decimal digits. Default: `None` (no rounding).
  - `allow_negative`: determines if negative values are allowed. Default: `False`.
* Example:
  ```python
  StructField("some_field", FloatType(), False, metadata={"length": (3, 5), "decimal_precision": 2})
  ```

### DecimalType()
* Possible values: `decimal.Decimal` values with specified precision and scale.
* Metadata params:
  - `allow_negative`: determines if negative values are allowed. Default: `False`.
* Example:
  ```python
  StructField("some_field", DecimalType(5, 2), False, metadata={"allow_negative": True})
  ```

### DateType() / TimestampType()
* Possible values: `datetime.date.Date` or `datetime.datetime.Datetime` objects.
* Metadata params:
  - `last`: interval string that determines range of the generated date. Default: `"1 year"`.
* Example:
  ```python
  StructField("some_field", DateType(), False, metadata={"last": "1 month"})
  ```

### StringType()
* Possible values: random strings
* Metadata params:
  - `content_type`: determines the kind of string to be generated. Default: `"random_ascii"`. See
  description of different content types below

#### StringType() content types:

###### `random_ascii`
  * Completely random string consisting of `ascii` symbols.
  * Metadata params:
  - `length`: number of symbols in string. An integer or tuple with min and max boundaries.
  Default: `(3, 20)`.
  - `case`: letter case. Possible values: `'ANY'`, `'CAPITALIZE'`, `'TITLE'`, `'UPPER'` or
  `'LOWER'`. Default: `ANY`.
  * Example:
    ```python
    StructField("some_field", StringType(), True, metadata={"length": 10, "case": "UPPER"})
    ```

###### `char`
  * A string with one random `ascii` symbol.
  * Metadata params:
  - `case`: letter case. Possible values: `'ANY'`, `'CAPITALIZE'`, `'TITLE'`, `'UPPER'` or
  `'LOWER'`. Default: `ANY`.
  * Example:
    ```python
    StructField("some_field", StringType(), True, metadata={"content_type": "char", "case": "LOWER"})
    ```

###### `phrase`
  * A string of one or more English words.
  * Metadata params:
  - `length`: number of words in phrase. An integer or tuple with min and max boundaries.
  Default: `(1, 3)`.
  - `case`: letter case. Possible values: `'ANY'`, `'CAPITALIZE'`, `'TITLE'`, `'UPPER'` or
  `'LOWER'`. Default: `LOWER`.
  * Example:
    ```python
    StructField("some_field", StringType(), True, metadata={"content_type": "phrase", "case": "TITLE"})
    ```

###### `text`
  * A string consisting of one or more sentences of English words.
  * Metadata params:
  - `length`: number of sentences in text. An integer or tuple with min and max boundaries.
  Default: `(3, 5)`.
  * Example:
    ```python
    StructField("some_field", StringType(), True, metadata={"content_type": "text", "length": (1, 2)})
    ```

###### `collector_number`
  * A string representing a collector number.
  * Example:
    ```python
    StructField("some_field", StringType(), True, metadata={"content_type": "collector_number"})
    ```

###### `json`
  * A random json string. Can use provided schema or default schema.
  * Metadata params:
  - `schema`: schema for json-encoded data structure
  Default: `None` (default schema)
  * Example:
    ```python
    json_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True, metadata={"length": 2})
    ])
    StructField(
        name="some_field", dataType=StringType(), nullable=True,
        metadata={"content_type": "json", "schema": json_schema})
    ```

###### `timestamp`
  * Date converted to a string.
  * Metadata params:
  - `last`: interval for generated dates from current timestamp backwards.
  Default: `"1 year"`
  - `date_format`: date format string (in SimpleDateFormat).
  Default: `"yyyy-MM-dd HH:mm:ss"`.
  * Example:
    ```python
    StructField("some_field", StringType(), True, metadata={"content_type": "timestamp", "date_format": "MMM dd, yyyy"})
    ```

###### `categorical`
  * Random value from specified list
  * Metadata params:
  - `categories`: list of categorical values ["phones", "tablets", "desktops"]
  * Example:
  ```python
  StructField("some_field", StringType(), True, metadata={"content_type": "categorical", "categories": ["phones", "tablets", "desktops"]})
  ```

### ArrayType()
* Possible values: list with arbitrary number of elements of specified type.
* Metadata params:
  - `length`: number of elements in the array. Integer or tuple of min and max range values.
  Default: `(0, 10)`.
  - `metadata`: metadata for array elements.
* Example:
  ```python
  StructField("some_field", ArrayType(StringType(), False), True, metadata={"length": 5, "metadata": "content_type": "random_ascii"})
  ```

### StructType()
* Possible values: dictionary with keys and data types based on provided schema.
* Example:
  ```python
  StructField(
      "some_field",
      StructType([
          StructField("id", IntegerType(), False, metadata={"length": 3}),
          StructField("name", StringType(), True, metadata={"length": 2, "content_type": "phrase", "case": "TITLE"})
      ]),
      True)
  ```

## DataGenerator

`DataGenerator` is an entry point to the application. It must be passed
a spark instance for instantiation. The object has a single method:
- `generate_df(schema, num_records=10000)` - returns a dataframe with
the specified number of records

To save generated data into files use `DataWriter` object available as
`generate` property of the `DataGenerator` object:

```python
DataGenerator(spark)\
    .generate\
    .save_to("/path/to/dir/")
```

To generate phrases and text snippets `DataGenerator` uses a dictionary
of 5,000 most common English words. To use an alternative dictionary
pass `dictionary` parameter with a list of words to use.

`null_prob` parameters determines the frequency of null values in
generated data for nullable fields. Example:

```python
DataGenerator(spark, dictionary=["abc", "def", "ghj"], null_prob=0.5)\
    .generate\
    .save_to("/path/to/dir/")
```

### DataWriter options

`DataWriter` can be customized with the following options:
- `num_files`: number of files to be generated. Default: `5`
- `num_records`: number of records in each file. Default: `10,000`
- `file_format`: file format to write (any format supported by Spark).
Default: `'csv'`
- `file_name_pattern`: pattern for naming files. Should contain text and
file date parts. File date part must be wrapped into square brackets and
use a subset of Java's SimpleDateFormat identifiers. Allowed identifiers:
`yyyy`, `yy`, `MM`, `dd`, `HH`, `mm`, `ss`. Default: `"test_data_[yyyyMMdd].csv"`
- `date_increment`: interval to increment file date. File dates will
start at current timestamp and go backwards at specified intervals.
Can be: `x year(s)`, `x month(s)`, `x week(s)`, `x day(s)`, `x hour(s)`,
`x minute(s)` or `x second(s)`. Default: `1 day`

Options can be set one by one with `option(key, val)` method or all at
once with `options(**kwargs)` method:

```python
DataGenerator(spark)\
    .generate\
    .option("num_files", 2)\
    .option("num_records", 10**6)\
    .save_to("/path/to/dir/")
```
or
```python
DataGenerator(spark)\
    .generate\
    .options(num_files=2, num_records=10**6)\
    .save_to("/path/to/dir/")
```

Additional options for Spark's `write` command can be specified with
`spark_option` or `spark_options` methods, e.g.

```python
DataGenerator(spark)\
    .generate\
    .options(num_files=2, num_records=10**6, file_format="csv")\
    .spark_option("header", True)\
    .save_to("/path/to/dir/")
```

### Nullification
Nullable fields are set to `null` with default probability of `10%`.
Default probability can be changed universally by passing `null_prob`
parameter to `DataGenerator` constructor or set for each field
individually with `null_prob` metadata parameter. `null_prob` value
must be a float between 0.0 and 1.0.

If column values should be null whenever values in another columns are
null it is possible to set `null_prob` parameter to a string with column
name.

Examples:
```python
# Change null_prob globally
DataGenerator(spark, null_prob=0.5).generate_df(schema)

# Set null_prob on per-column basis
schema = StructType([
    StructField("name", StringType(), True, metadata={"null_prob": 0.2}),
    StructField("title", StringType(), True, metadata={"null_prob": 0.8}),
    StructField("rating", IntegerType(), True, metadata={"null_prob": "title"})
])
```

### Word dictionary
To generate phrases `DataGenerator` uses a dictionary of 5,000 most common English words. This can be overridden by passing a custom list
to `DataGenerator`'s constructor. A list of 1,000 most common French
is available in `datagen.dictionary` module.

Examples:
```python
from datagen.dictionary import french_words

DataGenerator(spark, dictionary=french_words)...
# or
my_dictionary = ["ugh", "oh", "ah", "wow"]
DataGenerator(spark, dictionary=my_dictionary)...
```

### File system helper

`DataWriter` uses Spark to write data to files. Because of the way
Spark works files have to be renamed and moved after they are written
to the specified location. This is done by the `fs_helper` object specific
to the platform used. Currently, two fs_helpers are available: `'local'`
(for files saved to local environment) and `dbutils` (for Databricks
environment). `local` file system is enabled by default. To use `dbutils`
file helper one must pass it as parameter to the `fs_helper` method:

```python
DataGenerator(spark)\
    .generate\
    .fs_helper(dbutils)\
    .save_to("dbfs:/path/to/dir/")
```

### Limitations
* Generating data in batches is not supported. Data gets generated on the driver and then parallelized. It is possible to run out of memory when
generating very large amounts of data.
