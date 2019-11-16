import os
import datetime
import re

from .generator_utils import make_timedelta


class LocalFsHelper:
    """Class for file system helper object that can move
    and remove files and dirs on a local file system.

    Methods
    -------
    mv(from_path, to_path) - moves file from one location to another
    rmdir(dir_path) - recursively removes directory
    """

    def mv(self, from_path, to_path):
        """Method that moves file system object from one location to another.

        Parameters
        ----------
        from_path : str
            Full source path, including file name
        to_path : str
            Full destination path, including file name
        """
        os.replace(from_path, to_path)

    def rmdir(self, dir_path):
        """Method that recursively removes directory.

        Parameters
        ----------
        dir_path : str
            Path of directory to be removed
        """

        for obj in os.listdir(dir_path):
            obj_path = os.path.join(dir_path, obj)
            if os.path.isdir(obj_path):
                self.rmdir(obj_path)
            else:
                os.remove(obj_path)
        os.rmdir(dir_path)

    def ls(self, dir_path):
        """Method that returns a list of objects in a directory.

        Parameters
        ----------
        dir_path : str
            Path of directory

        Returns
        -------
        list of strings
        """
        return os.listdir(dir_path)


class DbUtilsFsHelper:
    """A wrapper for dbutils. Can only be used in Databricks.

    Parameters
    ----------
    dbutils : dbutils.DbUtils instance

    Methods
    -------
    mv(from_path, to_path) - moves file from one location to another
    rmdir(dir_path) - recursively removes directory
    """

    def __init__(self, dbutils):
        self.dbutils = dbutils

    def mv(self, from_path, to_path):
        """Method that moves file system object from one location to another.

        Parameters
        ----------
        from_path : str
            Full source path, including file name
        to_path : str
            Full destination path, including file name
        """
        self.dbutils.fs.mv(from_path, to_path)

    def rmdir(self, dir_path):
        """Method that recursively removes directory.

        Parameters
        ----------
        dir_path : str
            Path of directory to be removed
        """
        self.dbutils.fs.rm(dir_path, True)

    def ls(self, dir_path):
        """Method that returns a list of objects in a directory.

        Parameters
        ----------
        dir_path : str
            Path of directory

        Returns
        -------
        list of strings
        """
        return [obj.name for obj in self.dbutils.fs.ls(dir_path)]


def get_fs_helper(helper=None):
    """Factory function for file system helper objects.

    Parameters
    ----------
    helper : str or dbutils.DbUtils instance
        Either string 'local' or dbutils instance

    Returns
    -------
    LocalFsHelper or DbUtilsFsHelper instance

    Raises
    ------
    NotImplementedError
        If supplied helper param is neither 'local' nor dbutils
    """

    if helper is None or helper == "local":
        return LocalFsHelper()
    elif type(helper).__name__ == "DBUtils":
        return DbUtilsFsHelper(helper)
    raise NotImplementedError("No implementation for file system helper {}".format(helper))


def generate_file_names(name_pattern="test_data_[yyyyMMdd].csv",
                        every="1 day", num_files=10):
    """Function the generates a list of file names with encoded dates.

    Dates start at current timestamp and go backwards at intevals
    specified by 'every' parameter.

    Parameters
    ----------
    name_pattern : str
        File name pattern, e.g. 'file_name_[yyyyMMdd]'. Must include
        file date component wrapped in square brackets using a subset of
        Java's SimpleDateFormat date formats symbols.
        Allowed date format sequences: 'yy', 'yyyy', 'MM', 'dd', 'HH', 'mm', 'ss'.
        Default: 'test_data_[yyyyMMdd].csv'
    every : str
        Timedelta interval string, e.g. 2 days. Allowed values: 'x second(s)',
        'x year(s)', 'x month(s)', 'x week(s)', 'x day(s)', 'x hours(s)', 'x minutes(s)'
    num_files : str
        Number of file names to generate

    Returns
    -------
    tuple of strings
        Tuple with file names
    """

    def file_name_generator(name_pattern, ts, td):
        while True:
            file_name = generate_file_name(name_pattern, ts)
            yield file_name
            ts = ts - td

    def build_full_name(file_name, version):
        base_name, ext = os.path.splitext(file_name)
        version_suffix = "" if version == 0 else "_{}".format(str(version))
        return base_name + version_suffix + ext

    td = make_timedelta(every)
    fn_gen = file_name_generator(name_pattern, datetime.datetime.now(), td)

    file_names = []
    while len(file_names) < num_files:
        base_name = next(fn_gen)
        version = 0
        if base_name in [fn[0] for fn in file_names]:
            max_version = max([fn[1] for fn in file_names if fn[0] == base_name])
            version = max_version + 1
        file_names.append((base_name, version))

    return tuple([build_full_name(*fn) for fn in file_names])


def generate_file_name(pattern, ts):
    """Function that generates a single file name from pattern and timestamp.

    Parameters
    ----------
    pattern : str
        File name pattern, e.g. 'file_name_[yyyyMMdd]'. Must include
        file date component wrapped in square brackets using a subset of
        Java's SimpleDateFormat date formats symbols.
        Allowed date format sequences: 'yy', 'yyyy', 'MM', 'dd', 'HH', 'mm', 'ss'.
        Default: 'test_data_[yyyyMMdd].csv'
    ts : datatime.datetime
        Timestamp for file date

    Returns
    -------
    str
        File name
    """

    date_pattern = re.findall(r".*(\[[yMdmHs\-:_ ]*\]).*", pattern)
    if len(date_pattern) < 1:
        return pattern
    date_str = date_pattern[0]
    date_str = date_str\
        .replace("[", "").replace("]", "")\
        .replace("yyyy", "{:04d}".format(ts.year))\
        .replace("yy", "{:02d}".format(ts.year % 1000))\
        .replace("MM", "{:02d}".format(ts.month))\
        .replace("dd", "{:02d}".format(ts.day))\
        .replace("HH", "{:02d}".format(ts.hour))\
        .replace("mm", "{:02d}".format(ts.minute))\
        .replace("ss", "{:02d}".format(ts.second))

    file_name = pattern.replace(date_pattern[0], date_str)
    return file_name


def make_batch_sizes(num_records, max_batch_size):
    """Function that generates a sequence of batch sizes from
    total number of records and batch size.

    Parameters
    ----------
    num_records : int
        Overall number of records
    max_batch_size : int
        Number of records in a batch

    Returns
    -------
    tuple of integers
        Tuple with batch sizes (in terms of number of records)
    """

    if num_records <= max_batch_size:
        return tuple([num_records])
    nb = num_records / max_batch_size
    mbs = max_batch_size
    batches = [mbs for _ in range(int(nb))]
    remainder = int((nb % int(nb)) * mbs)
    if remainder > 0:
        batches += [remainder]
    return tuple(batches)
