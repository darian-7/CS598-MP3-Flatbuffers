# automatically generated by the FlatBuffers compiler, do not modify

# namespace: DataFrame

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# test
class DataFrame(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = DataFrame()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsDataFrame(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # DataFrame
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # DataFrame
    def Metadata(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # DataFrame
    def Columns(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from Project.DataFrame.Column import Column
            obj = Column()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # DataFrame
    def ColumnsLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # DataFrame
    def ColumnsIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        return o == 0

def DataFrameStart(builder):
    builder.StartObject(2)

def Start(builder):
    DataFrameStart(builder)

def DataFrameAddMetadata(builder, metadata):
    builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(metadata), 0)

def AddMetadata(builder, metadata):
    DataFrameAddMetadata(builder, metadata)

def DataFrameAddColumns(builder, columns):
    builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(columns), 0)

def AddColumns(builder, columns):
    DataFrameAddColumns(builder, columns)

def DataFrameStartColumnsVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)

def StartColumnsVector(builder, numElems):
    return DataFrameStartColumnsVector(builder, numElems)

def DataFrameEnd(builder):
    return builder.EndObject()

def End(builder):
    return DataFrameEnd(builder)