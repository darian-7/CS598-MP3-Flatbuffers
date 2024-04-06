# automatically generated by the FlatBuffers compiler, do not modify

# namespace: Dataframe

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class Dataframe(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Dataframe()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsDataframe(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # Dataframe
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Dataframe
    def Columns(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from Project.Dataframe.ColumnMetadata import ColumnMetadata
            obj = ColumnMetadata()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Dataframe
    def ColumnsLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Dataframe
    def ColumnsIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        return o == 0

    # Dataframe
    def Data(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from Project.Dataframe.ColumnDataHolder import ColumnDataHolder
            obj = ColumnDataHolder()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Dataframe
    def DataLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Dataframe
    def DataIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        return o == 0

def DataframeStart(builder):
    builder.StartObject(2)

def Start(builder):
    DataframeStart(builder)

def DataframeAddColumns(builder, columns):
    builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(columns), 0)

def AddColumns(builder, columns):
    DataframeAddColumns(builder, columns)

def DataframeStartColumnsVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)

def StartColumnsVector(builder, numElems):
    return DataframeStartColumnsVector(builder, numElems)

def DataframeAddData(builder, data):
    builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(data), 0)

def AddData(builder, data):
    DataframeAddData(builder, data)

def DataframeStartDataVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)

def StartDataVector(builder, numElems):
    return DataframeStartDataVector(builder, numElems)

def DataframeEnd(builder):
    return builder.EndObject()

def End(builder):
    return DataframeEnd(builder)
