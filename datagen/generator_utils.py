import random
import json
import string
import re
import datetime
from decimal import Decimal

from pyspark.sql.types import StringType


def validate_length_input(length_input, min_val, max_val):
    """Function that validates field length input.

    Parameters
    ----------
    length_input : int or tuple
        Field length or length range, e.g. 2 or (3, 5)
    min_val : int
        Minimal allowed value for length
    max_val : int
        Maximum allowed value for length

    Returns
    -------
    tuple
        Validated length range

    Raises
    ------
    ValueError
        If length fails validation
    """

    if isinstance(length_input, int):
        if length_input > max_val or min_val < min_val:
            raise ValueError("Length must be between {} and {}".format(min_val, max_val))
        length_input = (length_input, length_input)
    elif isinstance(length_input, (tuple, list)):
        if len(length_input) < 1:
            raise ValueError("Length range must not be empty")
        elif len(length_input) == 1:
            length_input = (length_input[0], max_val)
        else:
            length_input = (length_input[0], length_input[1])
        if not isinstance(length_input[0], int):
            raise ValueError("Invalid start of length range: {}".format(length_input[0]))
        elif not isinstance(length_input[1], int):
            raise ValueError("Invalid end of length range: {}".format(length_input[1]))
        elif (length_input[0] > max_val or length_input[0] < min_val) or\
             (length_input[1] > max_val or length_input[1] < min_val):
            raise ValueError("Length must be between {} and {}".format(min_val, max_val))
        elif length_input[0] > length_input[1]:
            raise ValueError("Start of length range cannot be greater than its end")
    return length_input


def apply_case(s, case):
    """Helper function that applies case to a string."""

    if case.upper() == "UPPER":
        s = s.upper()
    elif case.upper() == "LOWER":
        s = s.lower()
    elif case.upper() == "CAPITALIZE":
        s = s.capitalize()
    elif case.upper() == "TITLE":
        s = " ".join([w.capitalize() for w in s.split(" ")])
    return s


def nullify(val, probability=0.1):
    """Function that randomly sets value to null with specified probality.

    Parameters
    ----------
    val : any
        Original value
    probality : float
        Probability with which the value will be set to None.
        Must be in the range [0, 1]. Default: 0.1

    Returns
    -------
    any or None
    """

    if probability <= 0:
        return val
    elif probability >= 1:
        return None
    random_draw = random.random()
    return None if random_draw < probability else val


def make_boolean():
    """Function that generates value for a BooleanType field.
    Returns True or False.
    """
    return random.choice((True, False))


def make_n_length_integer(n1, n2, allow_negative=False):
    """Function that generates an integer with specified number of digits.

    Parameters
    ----------
    n1 : int
        Lower boundary for the number of digits
    n2 : int
        Upper boundary for the number of digits
    allow_negative : boolean
        Specifies if resulting integer can be negative. Default: False

    Returns
    -------
    int
    """

    range_start = 10**(n1-1)
    range_end = (10**n2)-1
    multiplier = random.choice((1, -1)) if allow_negative else 1
    return random.randint(range_start, range_end) * multiplier


def make_n_length_float(n1, n2, allow_negative=False, decimal_precision=None):
    """Function that generates a float with specified number
    of digits left of the point separator.

    Parameters
    ----------
    n1 : int
        Lower boundary for the number of non-decimal digits
    n2 : int
        Upper boundary for the number of non-decimal digits
    allow_negative : boolean
        Specifies if resulting float can be negative. Default: False
    decimal_precision : int or None
        Number of decimal digits. Default: None (no rounding)

    Returns
    -------
    float
    """

    range_start = 10**(n1-1)
    range_end = (10**n2)-1
    multiplier = random.choice((1, -1)) if allow_negative else 1
    val = random.uniform(range_start, range_end) * multiplier
    val = val if decimal_precision is None else round(val, decimal_precision)
    return val


def make_integer(length=(1, 9), allow_negative=False):
    """Function that generates a value for an IntegerType field.

    Parameters
    ----------
    length : int or tuple
        Number of digits, e.g. 2 or (3, 5)
    allow_negative : boolean
        Specifies if resulting integer can be negative. Default: False

    Returns
    -------
    int
        Integer with 1-9 digits
    """

    start, end = validate_length_input(length, min_val=1, max_val=9)
    return make_n_length_integer(start, end, allow_negative)


def make_long(length=(1, 18), allow_negative=False):
    """Function that generates a value for a LongType field.

    Parameters
    ----------
    length : int or tuple
        Number of digits, e.g. 2 or (3, 5)
    allow_negative : boolean
        Specifies if resulting integer can be negative. Default: False

    Returns
    -------
    int
        Integer with 1-18 digits
    """

    start, end = validate_length_input(length, min_val=1, max_val=18)
    return make_n_length_integer(start, end, allow_negative)


def make_byte(length=(1, 2), allow_negative=False):
    """Function that generates a value for an ByteType field.

    Parameters
    ----------
    length : int or tuple
        Number of digits, e.g. 1 or (1, 2)
    allow_negative : boolean
        Specifies if resulting integer can be negative. Default: False

    Returns
    -------
    int
        Integer with 1-2 digits
    """

    start, end = validate_length_input(length, min_val=1, max_val=2)
    return make_n_length_integer(start, end, allow_negative)


def make_collector_number():
    """Function that generates a value for a StringType field
    meant to represent collector number. Return a numeric string
    starting with '8' and 10 random digits.
    """
    return "8" + str(make_long(10))


def make_float(length=(1, 9), allow_negative=False, decimal_precision=None):
    """Function that generates a value for a FloatType field.

    Parameters
    ----------
    length : int or tuple
        Number of digits, e.g. 2 or (1, 3)
    allow_negative : boolean
        Specifies if resulting float can be negative. Default: False
    decimal_precision : int or None
        Number of decimal digits. Default: None (no rounding)

    Returns
    -------
    int
        Float with 1-9 non-decimal digits
    """

    start, end = validate_length_input(length, min_val=1, max_val=9)
    return make_n_length_float(start, end, allow_negative, decimal_precision)


def make_double(length=(1, 18), allow_negative=False, decimal_precision=None):
    """Function that generates a value for a DoubleType field.

    Parameters
    ----------
    length : int or tuple
        Number of digits, e.g. 2 or (1, 3)
    allow_negative : boolean
        Specifies if resulting float can be negative. Default: False
    decimal_precision : int or None
        Number of decimal digits. Default: None (no rounding)

    Returns
    -------
    int
        Float with 1-18 non-decimal digits
    """

    start, end = validate_length_input(length, min_val=1, max_val=18)
    return make_n_length_float(start, end, allow_negative, decimal_precision)


def make_decimal(precision=12, scale=3, allow_negative=False):
    """Function that generates a value for a DecimalType field.

    Parameters
    ----------
    precision : int
        Decimal precision (total number of digits)
    scale : int
        Decimal scale (number of decimal digits)
    allow_negative : boolean
        Specifies if resulting number can be negative. Default: False

    Returns
    -------
    decimal.Decimal
    """

    if scale > precision:
        msg = "Scale ({}) cannot be greater than precision ({})".format(scale, precision)
        raise ValueError(msg)
    val = make_double(
        length=(precision-scale),
        allow_negative=allow_negative,
        decimal_precision=scale
    )
    return Decimal("{}".format(round(val, precision)))


def make_string(length=(3, 20), case="ANY"):
    """Function that generates a value for a StringType field.

    Parameters
    ----------
    length : int or tuple
        Number of characters, e.g. 2 or (1, 3)
    case : str
        String case. Can be 'ANY', 'CAPITALIZE', 'TITLE', 'UPPER', 'LOWER'.
        Default: 'ANY'

    Returns
    -------
    str
        String of arbitrary length
    """

    start, end = validate_length_input(length, min_val=1, max_val=256)
    chars = string.ascii_letters
    output_length = random.randint(start, end)
    output = "".join([random.choice(chars) for _ in range(output_length)])
    output = apply_case(output, case)
    return output


def make_char(case="ANY"):
    """Function that generates a value for a CharType field.

    Parameters
    ----------
    case : str
        String case. Can be 'ANY', 'CAPITALIZE', 'TITLE', 'UPPER', 'LOWER'.
        Default: 'ANY'

    Returns
    -------
    str
        Single character
    """
    return make_string(length=1, case=case)


def make_phrase(dictionary, length=(1, 3), case="LOWER"):
    """Function that generates a value for a StringType field
    that represents a phrase.

    Parameters
    ----------
    dictionary : list or tuple
        List of words used to build a phrase
    length : int or tuple
        Number of words in a phrase, e.g. 2 or (1, 3)
    case : str
        String case. Can be 'ANY', 'CAPITALIZE', 'TITLE', 'UPPER', 'LOWER'.
        Default: 'ANY'

    Returns
    -------
    str
        One or several words
    """

    start, end = validate_length_input(length, min_val=1, max_val=10)
    phrase_length = random.randint(start, end)
    # dictionary is global var
    output = " ".join([random.choice(dictionary) for _ in range(phrase_length)])
    return apply_case(output, case)


def make_text(dictionary, length=(3, 5)):
    """Function that generates a value for a StringType field
    that represents text.

    Parameters
    ----------
    dictionary : list or tuple
        List of words used to build a phrase
    length : int or tuple
        Number of sentences in text, e.g. 2 or (1, 3)

    Returns
    -------
    str
        One or several sentences.
    """

    start, end = validate_length_input(length, min_val=2, max_val=10)
    num_sentences = random.randint(start, end)
    sentences = []
    for _ in range(num_sentences):
        sentence = make_phrase(
            dictionary=dictionary,
            length=(3, 7),
            case="CAPITALIZE") + "."
        sentences.append(sentence)
    return " ".join(sentences)


def make_cat_string(categories=(None,)):
    """Function that generates a value for a StringType field
    that can only take a limited number of values.

    Parameters
    ----------
    categories : list or tuple of strings
        Sequence of strings to select values from

    Returns
    -------
    str
    """
    return str(random.choice(categories))


def make_timedelta(interval_str):
    """Helper function that build timedelta obj from interval string."""

    periods = {"years": 0, "months": 0, "weeks": 0, "days": 0,
               "hours": 0, "minutes": 0, "seconds": 0}
    for key in periods:
        regex = r"(\d+)\s*{}?".format(key)
        finding = re.findall(regex, interval_str)
        if len(finding):
            periods[key] = int(finding[0])
    if periods["years"] > 0:
        periods["days"] += periods["years"] * 365
    if periods["months"]:
        periods["days"] += periods["months"] * 30
    del periods["years"]
    del periods["months"]
    return datetime.timedelta(**periods)


def make_date(last="1 year"):
    """Function that generates a value for a DateType field.

    Parameters
    ----------
    last : str
        Interval for generated date relative to current timestamp.
        Default: '1 year'

    Returns
    -------
    datetime.date.Date
    """

    return make_timestamp(last).date()


def make_timestamp(last="1 year"):
    """Function that generates a value for a TimestampType field.

    Parameters
    ----------
    last : str
        Interval for generated timestamp relative to current timestamp.
        Default: '1 year'

    Returns
    -------
    datetime.datetime.Datetime
    """

    end_range = datetime.datetime.now()
    start_range = end_range - make_timedelta(last)
    end_ts = int(end_range.timestamp())
    start_ts = int(start_range.timestamp())
    random_ts = random.randint(start_ts, end_ts)
    return datetime.datetime.fromtimestamp(random_ts)


def make_random_dict(dictionary, add_nested=False):
    """Function that generates a random dictionary with fixed structure.

    Parameters
    ----------
    dictionary : list or tuple
        List of words used to build phrases for some fields of the
        random dict
    add_nested : boolean
        Specifies if dict will contain nested dicts. Default: False

    Returns
    -------
    dict
    """

    random_dict = {
        "id": make_integer(),
        "name": make_phrase(dictionary=dictionary, length=2, case="TITLE"),
        "date": datetime.datetime.strftime(make_timestamp(), "%Y-%m-%d %H:%M:%S"),
        "tags": [make_phrase(dictionary, length=1) for _ in range(random.randint(1, 10))],
        "versions": [random.randint(1, 10) for _ in range(random.randint(1, 5))],
    }
    if add_nested:
        num_keys = random.randint(0, 10)
        random_keys = [make_phrase(dictionary, length=1) for _ in range(num_keys)]
        random_vals = [make_random_dict(dictionary, add_nested=False) for _ in range(num_keys)]
        random_dict["related"] = {k: v for (k, v) in zip(random_keys, random_vals)}
    return random_dict


def make_json(dictionary):
    """Function that generates a random json string.

    Parameters
    ----------
    dictionary : list or tuple
        List of words used to build phrases inside jsonified dictionary

    Returns
    -------
    str
        Json-encoded string
    """
    return json.dumps(make_random_dict(dictionary, add_nested=True))


def make_array(dictionary,
               length=(0, 10),
               elem_type=StringType(),
               nullable=False,
               null_prob=0.1,
               metadata={}):
    """Function that generates a random array of elements
    with specified type for ArrayType field.

    Parameters
    ----------
    dictionary : list or tuple
        List of words for phrase or text strings inside array
    length : int or tuple
        Array lenght or range, e.g. 2 or (1, 3)
    elem_type : pyspark.sql.types.*
        Element data type. Default: StringType()
    nullable : boolean
        Specifies if elements can be None
    null_prob : float
        Frequency of null values for nullable fields. Default: 0.1
    metadata : dict
        Element fields' metadata
    """

    start, end = validate_length_input(length, min_val=0, max_val=10)
    num_elems = random.randint(start, end)
    return [generate_val(elem_type, dictionary, nullable, null_prob, metadata) for _ in range(num_elems)]


def make_struct(struct_schema, dictionary, null_prob):
    """Function that generates value for StructType field.
    Return dictionary of values. Alias for generate_row().
    """

    return generate_row(struct_schema, dictionary, null_prob)


def generate_batch(schema, dictionary, batch_size=10000, null_prob=0.1):
    """Function that generates a batch of records.

    Parameters
    ----------
    schema : pyspark.sql.types.StructType
        Data schema
    dictionary : list or tuple
        List of words for phrase or text strings
    batch_size : int
        Number of records in batch. Default: 10000
    null_prob : float
        Frequency of null values for nullable fields. Default: 0.1

    Returns
    -------
    List of dicts
    """

    return [generate_row(schema, dictionary, null_prob) for _ in range(batch_size)]


def generate_row(schema, dictionary, null_prob):
    """Function that generates value for StructType field.
    Return dictionary of values.

    Parameters
    ----------
    schema : pyspark.sql.types.StructType
        Data schema
    dictionary : list or tuple
        List of words for phrase or text strings
    null_prob : float
        Frequency of null values for nullable fields

    Returns
    -------
    dict
    """

    row = {}
    for field in schema.fields:
        row[field.name] = generate_val(
            data_type=field.dataType,
            nullable=field.nullable,
            metadata=field.metadata,
            dictionary=dictionary,
            null_prob=null_prob)
    return row


def generate_val(data_type, dictionary, nullable=False, null_prob=0.1, metadata={}):
    """Function that generates a value for specified data type.

    Parameters
    ----------
    data_type : pyspark.sql.types.*
        Spark data type
    dictionary : list or tuple
        List of words for phrase or text strings
    nullable : boolean
        Determines it value can occasionaly be None
    null_prob : float
        Frequency of null values for nullable fields. Default: 0.1
    metadata : dict
        Field metadata

    Returns
    -------
    any
        Generated field value
    """

    def set_params(metadata, keys):
        params = {}
        for key in keys:
            if metadata.get(key) is not None:
                params[key] = metadata.get(key)
        return params

    val = None
    func = None
    params = {}
    func_mapping = {
        "integer": {"func": make_integer, "params": ("length", "allow_negative")},
        "long": {"func": make_long, "params": ("length", "allow_negative")},
        "byte": {"func": make_byte, "params": ("length", "allow_negative")},
        "float": {"func": make_float, "params": ("length", "allow_negative", "decimal_precision")},
        "double": {
            "func": make_double,
            "params": ("length", "allow_negative", "decimal_precision")
        },
        "date": {"func": make_date, "params": ("last",)},
        "timestamp": {"func": make_timestamp, "params": ("last",)},
        "boolean": {"func": make_boolean, "params": tuple()},
        "string": {"func": make_string, "params": tuple()},
        "decimal": {"func": make_decimal, "params": ("allow_negative")},
        "array": {"func": make_array, "params": ("length", "metadata")},
        "struct": {"func": make_struct, "params": tuple()}
    }
    func = func_mapping[data_type.typeName()]["func"]
    params = set_params(metadata, func_mapping[data_type.typeName()]["params"])

    # Extra logic for strings, decimals, arrays and structs
    if data_type.typeName() == "string":
        content_type = metadata.get("content_type", "random_ascii")
        string_func_mapping = {
            "random_ascii": {"func": make_string, "params": ("length", "case")},
            "phrase": {"func": make_phrase, "params": ("length", "case")},
            "text": {"func": make_text, "params": ("length",)},
            "char": {"func": make_char, "params": ("case",)},
            "categorical": {"func": make_cat_string, "params": ("categories",)},
            "collector_number": {"func": make_collector_number, "params": tuple()},
            "json": {"func": make_json, "params": tuple()}
        }
        func = string_func_mapping[content_type]["func"]
        params = set_params(metadata, string_func_mapping[content_type]["params"])
        # Pass dictionary as param to certain content types
        if content_type in ("phrase", "text", "json"):
            params["dictionary"] = dictionary
    elif data_type.typeName() == "decimal":
        if getattr(data_type, "scale", None) is not None:
            params["scale"] = getattr(data_type, "scale")
        if getattr(data_type, "precision", None) is not None:
            params["precision"] = getattr(data_type, "precision")
    elif data_type.typeName() == "array":
        params["elem_type"] = data_type.elementType
        params["nullable"] = data_type.containsNull
        params["dictionary"] = dictionary
        params["null_prob"] = null_prob
    elif data_type.typeName() == "struct":
        params["struct_schema"] = data_type
        params["dictionary"] = dictionary
        params["null_prob"] = null_prob

    # Exit if func not found
    if not func:
        return val
    val = func(**params)
    val = nullify(val, null_prob) if nullable else val
    return val
