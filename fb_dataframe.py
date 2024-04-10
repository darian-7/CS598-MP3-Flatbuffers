import flatbuffers
import pandas as pd
import struct
import time
import types
import numpy as np

# Your Flatbuffer imports here (i.e. the files generated from running ./flatc with your Flatbuffer definition)...
# from Project.Dataframe import Dataframe, ColumnMetadata, Int64Column, FloatColumn, StringColumn, DataType, ColumnDataHolder
# from Project.Dataframe.DataType import DataType

from Project.Dataframe import Dataframe
from DataFrame import Column
from DataFrame import Metadata
from Project.Dataframe.DataType import DataType


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
    for col_name, col_data in df.items():
        # Determine the data type of the column.
        if col_data.dtype == 'int64':
            dtype = DataType.Int64
            # Serialize the integer data.
            Column.ColumnStartIntValuesVector(builder, len(col_data))
            for value in reversed(col_data):
                builder.PrependInt64(value)
            values_offset = builder.EndVector(len(col_data))
        elif col_data.dtype == 'float64':
            dtype = DataType.Float
            # Serialize the float data.
            Column.ColumnStartFloatValuesVector(builder, len(col_data))
            for value in reversed(col_data):
                builder.PrependFloat64(value)
            values_offset = builder.EndVector(len(col_data))
        elif col_data.dtype == 'object':
            dtype = DataType.String
            # Serialize the string data.
            string_offsets = [builder.CreateString(str(value)) for value in reversed(col_data)]
            Column.ColumnStartStringValuesVector(builder, len(col_data))
            for offset in string_offsets:
                builder.PrependUOffsetTRelative(offset)
            values_offset = builder.EndVector(len(col_data))
        else:
            continue  # Unsupported data type

        # Serialize the column metadata.
        Metadata.MetadataStart(builder)
        name_offset = builder.CreateString(col_name)
        Metadata.MetadataAddName(builder, name_offset)
        Metadata.MetadataAddDtype(builder, dtype)
        metadata_offset = Metadata.MetadataEnd(builder)

        # Create the column.
        Column.ColumnStart(builder)
        Column.ColumnAddMetadata(builder, metadata_offset)
        if dtype == DataType.Int64:
            Column.ColumnAddIntValues(builder, values_offset)
        elif dtype == DataType.Float:
            Column.ColumnAddFloatValues(builder, values_offset)
        elif dtype == DataType.String:
            Column.ColumnAddStringValues(builder, values_offset)
        column_offsets.append(Column.ColumnEnd(builder))

    # Create a vector of columns in the DataFrame.
    Dataframe.DataframeStartColumnsVector(builder, len(column_offsets))
    for offset in reversed(column_offsets):
        builder.PrependUOffsetTRelative(offset)
    columns_offset = builder.EndVector(len(column_offsets))

    # Serialize the DataFrame.
    Dataframe.DataframeStart(builder)
    Dataframe.DataframeAddColumns(builder, columns_offset)
    dataframe_offset = Dataframe.DataframeEnd(builder)

    builder.Finish(dataframe_offset)
    return bytes(builder.Output())


    # builder = flatbuffers.Builder(1024)

    # columns_metadata_offsets = []
    # column_data_offsets = []

    # for column_name in df.columns:  # Directly use the order of columns in the DataFrame
    #     data = df[column_name]
    #     name_offset = builder.CreateString(column_name)

    #     if data.dtype == 'int64':
    #         data_type = DataType.Int64
    #         builder.StartVector(8, len(data), 8)
    #         for val in reversed(data.to_numpy()):
    #             builder.PrependInt64(val)
    #         values_vector_offset = builder.EndVector(len(data))
    #         Int64Column.Start(builder)
    #         Int64Column.AddValues(builder, values_vector_offset)
    #         data_offset = Int64Column.End(builder)
    #     elif data.dtype == 'float':
    #         data_type = DataType.Float
    #         builder.StartVector(8, len(data), 8)
    #         for val in reversed(data.to_numpy()):
    #             builder.PrependFloat64(val)
    #         values_vector_offset = builder.EndVector(len(data))
    #         FloatColumn.Start(builder)
    #         FloatColumn.AddValues(builder, values_vector_offset)
    #         data_offset = FloatColumn.End(builder)
    #     elif data.dtype == 'object':  # Assuming object dtype for strings
    #         data_type = DataType.String
    #         strings_offsets = [builder.CreateString(str(value)) for value in data]
    #         builder.StartVector(4, len(strings_offsets), 4)
    #         for offset in reversed(strings_offsets):
    #             builder.PrependUOffsetTRelative(offset)
    #         values_vector_offset = builder.EndVector(len(strings_offsets))
    #         StringColumn.Start(builder)
    #         StringColumn.AddValues(builder, values_vector_offset)
    #         data_offset = StringColumn.End(builder)
    #     else:
    #         continue  # Skip unsupported data types

    #     ColumnMetadata.Start(builder)
    #     ColumnMetadata.AddName(builder, name_offset)
    #     ColumnMetadata.AddType(builder, data_type)
    #     metadata_offset = ColumnMetadata.End(builder)

    #     ColumnDataHolder.Start(builder)
    #     ColumnDataHolder.AddData(builder, data_offset)
    #     column_data_holder_offset = ColumnDataHolder.End(builder)

    #     columns_metadata_offsets.append(metadata_offset)
    #     column_data_offsets.append(column_data_holder_offset)

    # # Create vectors for the metadata and data
    # Dataframe.StartColumnsVector(builder, len(columns_metadata_offsets))
    # for offset in reversed(columns_metadata_offsets):
    #     builder.PrependUOffsetTRelative(offset)
    # columns_metadata_vector = builder.EndVector(len(columns_metadata_offsets))

    # Dataframe.StartDataVector(builder, len(column_data_offsets))
    # for offset in reversed(column_data_offsets):
    #     builder.PrependUOffsetTRelative(offset)
    # columns_data_vector = builder.EndVector(len(column_data_offsets))

    # # Create the main Dataframe object
    # Dataframe.Start(builder)
    # Dataframe.AddColumns(builder, columns_metadata_vector)
    # Dataframe.AddData(builder, columns_data_vector)
    # dataframe_offset = Dataframe.End(builder)

    # builder.Finish(dataframe_offset)
    # return bytes(builder.Output())

    


def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    """
        Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
        similar to df.head(). If there are less than n rows, return the entire Dataframe.
        Hint: don't forget the column names!

        @param fb_bytes: bytes of the Flatbuffer Dataframe.
        @param rows: number of rows to return.
    """
    return pd.DataFrame()  # REPLACE THIS WITH YOUR CODE...


def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    """
        Applies GROUP BY SUM operation on the flatbuffer dataframe grouping by grouping_col_name
        and summing sum_col_name. Returns the aggregate result as a Pandas dataframe.

        @param fb_bytes: bytes of the Flatbuffer Dataframe.
        @param grouping_col_name: column to group by.
        @param sum_col_name: column to sum.
    """
    return pd.DataFrame()  # REPLACE THIS WITH YOUR CODE...


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
    