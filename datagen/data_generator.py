import os

from pyspark.sql.types import StructType, ArrayType, StructField
from pyspark.sql import functions as f

from .dictionary import english_words as DEFAULT_DICTIONARY
from .generator_utils import generate_batch, set_counters, reset_counters
from .fs_utils import generate_file_names, get_fs_helper
import datagen.generator_utils as gutils


class DataGenerator:
    """Fake data generator class.

    Generates dataframes and files with fake data.

    Parameters
    ----------
    spark : spark instance
        Instance of Apache spark session
    dictionary : list[str]
        List of words to generate phrases from. Default: None.
        If None default dictionary of 5,000 most common English
        words is used.
    null_prob : float
        Global null probability for nullable fields within (0, 1)
        range. Default: 0.1

    Methods
    -------
    generate_df(schema, num_records) - generates dataframe from schema
    generate - return DataWriter object to generate data and write to files
    """

    def __init__(self, spark, dictionary=None, null_prob=0.1):
        self.spark = spark
        self.dictionary = dictionary or DEFAULT_DICTIONARY
        self.null_prob = null_prob
        gutils._counters = {}

    def generate_df(self, schema, num_records=10000, ignore_counters=False):
        """Method that generates a dataframe with fake data from schema.

        Parameters
        ----------
        schema : pyspark.sql.types.StructType
            Schema of dataframe to be generated
        num_records : int
            Number of records to generate. Default: 10000

        Returns
        -------
        dataframe
        """
        if not ignore_counters:
            set_counters(schema)
        data = generate_batch(
            schema=schema,
            batch_size=num_records,
            dictionary=self.dictionary,
            null_prob=self.null_prob)
        if not ignore_counters:
            reset_counters()
        # Remove metadata from schema as params may not be json-serializable
        output_schema = clean_schema(schema)
        df = self.spark.createDataFrame(data, output_schema)
        df = nullify_cols(df, schema)
        return df

    @property
    def generate(self):
        """Method that returns a DataWriter object that can generate
        data and write it to files."""

        return DataWriter(self)


class DataWriter:
    """Class for objects that generate fake data and save it
    to files.

    Parameters
    ----------
    data_generator : DataGenerator instance
        Usually the instantiating object will pass itself

    Methods
    -------
    option(option_name, option_val) - sets option
    spark_option(option_name, option_val) - sets option for spark's write method
    schema(schema) - sets data schema
    fs_helper(helper) - sets fs_helper
    save_to(location) - generates data and saves to file(s)
    """

    def __init__(self, data_generator):
        self.data_generator = data_generator
        self._options = {
            "num_files": 5,
            "num_records": 10000,
            "file_format": "csv",
            "file_name_pattern": "test_data_[yyyyMMdd].csv",
            "date_increment": "1 day"
        }
        self._spark_options = {
            "header": True
        }
        self._schema = None
        self._fs_helper = "local"

    def option(self, option_name, option_val):
        """Method that sets an option.

        Parameters
        ----------
        option_name : str
            Name of the option
        option_val : any
            Option value

        Returns
        -------
        self
        """
        self._options[option_name] = option_val
        return self

    def options(self, **kwargs):
        """Method that sets multiple options.

        Parameters
        ----------
        **kwargs

        Returns
        -------
        self
        """
        self._options.update(kwargs)
        return self

    def spark_option(self, option_name, option_val):
        """Method that sets an option for spark dataframe write method.

        Parameters
        ----------
        option_name : str
            Name of the option
        option_val : any
            Option value

        Returns
        -------
        self
        """
        self._spark_options[option_name] = option_val
        return self

    def spark_options(self, **kwargs):
        """Method that sets options for spark dataframe write method.

        Parameters
        ----------
        **kwargs

        Returns
        -------
        self
        """
        self._spark_options.update(kwargs)
        return self

    def schema(self, schema):
        """Method that sets a schema for generated data.

        Parameters
        ----------
        schema : pyspark.sql.types.StructType
            Data schema

        Returns
        -------
        self
        """
        self._schema = schema
        return self

    def fs_helper(self, helper):
        """Method that defines file system helper object to be used
        to rename and cliean up files after they are generated.

        Parameters
        ----------
        helper : 'local' str or dbutils.DbUtils instance
            Fs helper object or identifier

        Returns
        -------
        self
        """
        self._fs_helper = helper
        return self

    def save_to(self, location):
        """Method that generates data and saves to specified location.

        Parameters
        ----------
        location : str
            Directory where files should be saved

        Raises
        ------
        RuntimeError
            If schema parameter not set
        """

        if not self._schema:
            raise RuntimeError("Data schema not set. Use .schema(schema) method.")
        file_names = generate_file_names(
            name_pattern=self._options.get("file_name_pattern"),
            every=self._options.get("date_increment"),
            num_files=self._options.get("num_files")
        )
        set_counters(self._schema)
        for file_name in file_names[::-1]:
            # Generate batch for file
            batch_df = self.data_generator.generate_df(
                schema=self._schema,
                num_records=self._options.get("num_records"),
                ignore_counters=True
            ).coalesce(1)
            # Configure writer
            spark_writer = batch_df.write\
                .format(self._options.get("file_format"))\
                .mode("overwrite")
            # Write file
            for key, val in self._spark_options.items():
                spark_writer.option(key, val)
            tmp_dir = os.path.join(location, "tmp", file_name)
            spark_writer.save(tmp_dir)
            # Rename files and clean up
            fs_helper = get_fs_helper(self._fs_helper)
            to_path = os.path.join(location, file_name)
            csv_file_name = [obj for obj in fs_helper.ls(tmp_dir) if obj.startswith("part-")][0]
            from_path = os.path.join(tmp_dir, csv_file_name)
            fs_helper.mv(from_path, to_path)
            fs_helper.rmdir(os.path.join(location, "tmp"))

        reset_counters()


def clean_schema(schema):
    """Function that recursively removes metadata from schema fields and returns
    new schema without modifying the original one."""

    new_schema = StructType([])
    for field in schema.fields:
        new_dtype = field.dataType
        if new_dtype.typeName() == 'struct':
            new_dtype = clean_schema(new_dtype)
        elif new_dtype.typeName() == 'array' and new_dtype.elementType.typeName() == 'struct':
            new_dtype = ArrayType(clean_schema(new_dtype.elementType), new_dtype.containsNull)
        new_field = StructField(field.name, new_dtype, field.nullable, {})
        new_schema.add(new_field)
    return new_schema


def nullify_cols(df, schema):
    """Function that nullifies values in related columns.

    If col A depends on B for nullification its values will
    be set to null whenever B is null.
    """

    for field in schema:
        null_prob = field.metadata.get("null_prob")
        model_col = null_prob if isinstance(null_prob, str) else None
        if model_col and model_col in df.columns:
            val_expr = f.when(f.col(model_col).isNull(), f.lit(None)).otherwise(f.col(field.name))
            df = df.withColumn(field.name, val_expr)
    return df
