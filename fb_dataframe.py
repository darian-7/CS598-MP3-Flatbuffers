import flatbuffers
import pandas as pd
import struct
import time
import types
import numpy as np

# Your Flatbuffer imports here (i.e. the files generated from running ./flatc with your Flatbuffer definition)...
from flatbuffers import Builder
from Project.DataFrame import DataFrame, Column, Metadata, ValueType
from Project.DataFrame.DataType import DataType

def to_flatbuffer(df: pd.DataFrame) -> bytes:
    """
        Converts a DataFrame to a flatbuffer. Returns the bytes of the flatbuffer.

        The flatbuffer should follow a columnar format as follows:
        +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
        | DF metadata | col 1 metadata | val 1 | val 2 | ... | col 2 metadata | val 1 | val 2 | ... |
        +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
        You are free to put any bookkeeping items in the metadata. however, for autograding purposes:
        1. Make sure that the values in the columns are laid out in the flatbuffer as specified above
        2. Serialize int and float values using flatbuffer's 'PrependInt64' and 'PrependFloat64'
            functions, respectively (i.e., don't convert them to strings yourself - you will lose
            precision for floats).

        @param df: the dataframe.
    """
    builder = flatbuffers.Builder(1024)

    # Serialize columns and collect their offsets.
    column_offsets = []
    for col_name in df[df.columns[::-1]]:  # Use the order of columns as in DataFrame
        col_data = df[col_name]
        dtype = None
        values_offset = None

        # Create the column name string outside of any other object creation to avoid nesting.
        name_offset = builder.CreateString(col_name)

        if col_data.dtype == 'int64':
            dtype = DataType.Int64
            Column.ColumnStartIntValuesVector(builder, len(col_data))
            for value in col_data.iloc[::-1]:  # reverse iterate using iloc
                builder.PrependInt64(value)
            values_offset = builder.EndVector(len(col_data))
        elif col_data.dtype == 'float64':
            dtype = DataType.Float
            Column.ColumnStartFloatValuesVector(builder, len(col_data))
            for value in col_data.iloc[::-1]:  # reverse iterate using iloc
                builder.PrependFloat64(value)
            values_offset = builder.EndVector(len(col_data))
        elif col_data.dtype == 'object':
            dtype = DataType.String
            # Pre-create strings for column values to avoid nested construction
            string_offsets = [builder.CreateString(str(value)) for value in col_data.iloc[::-1]]
            Column.ColumnStartStringValuesVector(builder, len(col_data))
            for offset in string_offsets:
                builder.PrependUOffsetTRelative(offset)
            values_offset = builder.EndVector(len(col_data))

        Metadata.MetadataStart(builder)
        Metadata.MetadataAddName(builder, name_offset)
        Metadata.MetadataAddDtype(builder, dtype)
        metadata_offset = Metadata.MetadataEnd(builder)

        Column.ColumnStart(builder)
        Column.ColumnAddMetadata(builder, metadata_offset)
        if dtype == DataType.Int64:
            Column.ColumnAddIntValues(builder, values_offset)
        elif dtype == DataType.Float:
            Column.ColumnAddFloatValues(builder, values_offset)
        elif dtype == DataType.String:
            Column.ColumnAddStringValues(builder, values_offset)
        column_offsets.append(Column.ColumnEnd(builder))

    DataFrame.DataFrameStartColumnsVector(builder, len(column_offsets))
    for offset in reversed(column_offsets):
        builder.PrependUOffsetTRelative(offset)
    columns_offset = builder.EndVector(len(column_offsets))

    DataFrame.DataFrameStart(builder)
    DataFrame.DataFrameAddColumns(builder, columns_offset)
    dataframe_offset = DataFrame.DataFrameEnd(builder)

    builder.Finish(dataframe_offset)
    return builder.Output()
    

def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    """
    Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
    similar to df.head(). If there are less than n rows, return the entire Dataframe.
    Hint: don't forget the column names!

    @param fb_bytes: bytes of the Flatbuffer Dataframe.
    @param rows: number of rows to return.
    """
    root_df = DataFrame.DataFrame.GetRootAsDataFrame(fb_bytes, 0)
    column_count = root_df.ColumnsLength()

    # get column metadata and reverse order of columns to match the original df
    column_metadata = [(root_df.Columns(i).Metadata().Name().decode('utf-8'), root_df.Columns(i).Metadata().Dtype(), i)
                       for i in range(column_count)][::-1]

    # dict to hold colunm data
    data = {meta[0]: [] for meta in column_metadata}

    # get min rows across all columns to handle columns with diff. lengths
    rows_available = float('inf')
    for name, dtype, index in column_metadata:
        col = root_df.Columns(index)
        if dtype == ValueType.ValueType.Int:
            rows_available = min(rows_available, col.IntValuesLength())
        elif dtype == ValueType.ValueType.Float:
            rows_available = min(rows_available, col.FloatValuesLength())
        elif dtype == ValueType.ValueType.String:
            rows_available = min(rows_available, col.StringValuesLength())

    # get data for each column up to the min of available and requested rows
    rows_to_fetch = min(rows, rows_available)
    for row_idx in range(rows_to_fetch):
        for name, dtype, index in column_metadata:
            col = root_df.Columns(index)
            if dtype == ValueType.ValueType.Int:
                data[name].append(col.IntValues(row_idx))
            elif dtype == ValueType.ValueType.Float:
                data[name].append(col.FloatValues(row_idx))
            elif dtype == ValueType.ValueType.String:
                data[name].append(col.StringValues(row_idx).decode('utf-8'))

    # make pd df with reversed column order
    df_result = pd.DataFrame(data, columns=[meta[0] for meta in column_metadata])
    return df_result


def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    """
        Applies GROUP BY SUM operation on the flatbuffer dataframe grouping by grouping_col_name
        and summing sum_col_name. Returns the aggregate result as a Pandas dataframe.

        @param fb_bytes: bytes of the Flatbuffer Dataframe.
        @param grouping_col_name: column to group by.
        @param sum_col_name: column to sum.
    """

    df = DataFrame.DataFrame.GetRootAsDataFrame(fb_bytes, 0)
    data = {}

    for i in range(df.ColumnsLength()):
        col = df.Columns(i)
        col_name = col.Metadata().Name().decode('utf-8')
        if col_name in [grouping_col_name, sum_col_name]:
            if col.Metadata().Dtype() == ValueType.ValueType().Int:
                data[col_name] = [col.IntValues(j) for j in range(col.IntValuesLength())]
            elif col.Metadata().Dtype() == ValueType.ValueType().Float:
                data[col_name] = [col.FloatValues(j) for j in range(col.FloatValuesLength())]
            elif col.Metadata().Dtype() == ValueType.ValueType().String:
                data[col_name] = [col.StringValues(j).decode('utf-8') for j in range(col.StringValuesLength())]

    # make df from dict
    df = pd.DataFrame(data)
    result = df.groupby(grouping_col_name).sum()

    return result


def fb_dataframe_map_numeric_column(fb_buf: memoryview, col_name: str, map_func: types.FunctionType) -> None:
    """
        Apply map_func to elements in a numeric column in the Flatbuffer Dataframe in place.
        This function shouldn't do anything if col_name doesn't exist or the specified
        column is a string column.

        @param fb_buf: buffer containing bytes of the Flatbuffer Dataframe.
        @param col_name: name of the numeric column to apply map_func to.
        @param map_func: function to apply to elements in the numeric column.
    """
    # YOUR CODE HERE...
    pass
    