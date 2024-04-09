import flatbuffers
import pandas as pd
import struct
import time
import types
import numpy as np

# Your Flatbuffer imports here (i.e. the files generated from running ./flatc with your Flatbuffer definition)...
# from Project.Dataframe import Dataframe, ColumnMetadata, Int64Column, FloatColumn, StringColumn, DataType, ColumnDataHolder
# from Project.Dataframe.DataType import DataType

from Project.Dataframe import MyDataframe


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

    builder = flatbuffers.Builder(1024)

    # Serialize columns and collect their metadata
    column_metadata = []
    column_data = []
    for col_name, dtype in df.dtypes.iteritems():
        name = builder.CreateString(col_name)
        if dtype == 'int64':
            MyDataframe.Int64ColumnStartValuesVector(builder, len(df[col_name]))
            for value in df[col_name][::-1]:  # FlatBuffers builds arrays backwards
                builder.PrependInt64(value)
            values = builder.EndVector(len(df[col_name]))
            MyDataframe.Int64ColumnStart(builder)
            MyDataframe.Int64ColumnAddValues(builder, values)
            column = MyDataframe.Int64ColumnEnd(builder)
            column_type = MyDataframe.DataType.Int64
        elif dtype == 'float':
            MyDataframe.FloatColumnStartValuesVector(builder, len(df[col_name]))
            for value in df[col_name][::-1]:
                builder.PrependFloat64(value)
            values = builder.EndVector(len(df[col_name]))
            MyDataframe.FloatColumnStart(builder)
            MyDataframe.FloatColumnAddValues(builder, values)
            column = MyDataframe.FloatColumnEnd(builder)
            column_type = MyDataframe.DataType.Float
        elif dtype == 'object':
            strings = [builder.CreateString(str(value)) for value in df[col_name]]
            MyDataframe.StringColumnStartValuesVector(builder, len(df[col_name]))
            for s in strings[::-1]:
                builder.PrependUOffsetTRelative(s)
            values = builder.EndVector(len(df[col_name]))
            MyDataframe.StringColumnStart(builder)
            MyDataframe.StringColumnAddValues(builder, values)
            column = MyDataframe.StringColumnEnd(builder)
            column_type = MyDataframe.DataType.String
        else:
            raise ValueError(f"Unsupported dtype: {dtype}")

        MyDataframe.ColumnMetadataStart(builder)
        MyDataframe.ColumnMetadataAddName(builder, name)
        MyDataframe.ColumnMetadataAddType(builder, column_type)
        metadata = MyDataframe.ColumnMetadataEnd(builder)

        column_metadata.append(metadata)
        column_data.append(column)

    # Create vectors for column metadata and data
    MyDataframe.DataframeStartColumnsVector(builder, len(column_metadata))
    for m in column_metadata[::-1]:
        builder.PrependUOffsetTRelative(m)
    columns = builder.EndVector(len(column_metadata))

    MyDataframe.DataframeStartDataVector(builder, len(column_data))
    for d in column_data[::-1]:
        builder.PrependUOffsetTRelative(d)
    data = builder.EndVector(len(column_data))

    # Create the Dataframe
    MyDataframe.DataframeStart(builder)
    MyDataframe.DataframeAddColumns(builder, columns)
    MyDataframe.DataframeAddData(builder, data)
    dataframe = MyDataframe.DataframeEnd(builder)

    builder.Finish(dataframe)
    return builder.Output()


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
    