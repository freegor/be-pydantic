from uuid import UUID
import pyarrow as pa
import jsonpickle
from pydantic import BaseModel, constr, validator
import pandas


class CustomBaseModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            pandas.DataFrame: lambda v: serialize_with_pyarrow(v)
        }


class Dataset(CustomBaseModel):
    id: str
    name: constr(max_length=128)
    dataframe: pandas.DataFrame

    @validator('id')
    def is_uuid4_string(cls, value):
        try:
            UUID(value, version=4)
        except ValueError as ve:
            raise ValueError('The id value is not uuid4') from ve
        return value


def serialize_with_pyarrow(dataframe: pandas.DataFrame):
    batch = pa.record_batch(dataframe)
    write_options = pa.ipc.IpcWriteOptions(compression="zstd")
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema,   options=write_options) as writer:
        writer.write_batch(batch)
    pybytes = sink.getvalue().to_pybytes()
    pybytes_str = jsonpickle.encode(pybytes, unpicklable=True, make_refs=False)
    return pybytes_str


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    data = {'Name': ['Tom', 'nick', 'krish', 'jack'], 'Age': [20, 21, 19, 18]}
    df = pandas.DataFrame(data=data)
    dataset = Dataset(id='8fba0c5b-4792-4bc1-a8d6-3eea6cc5d086', name='ppl_dataset', dataframe=df)
    encoded_dataset = dataset.json()
    print(encoded_dataset)

# {"id": "8fba0c5b-4792-4bc1-a8d6-3eea6cc5d086", "name": "ppl_dataset", "dataframe": "{\"py/b64\":
# \"//////gCAAAQAAAAAAAKAA4ABgAFAAgACgAAAAABBAAQAAAAAAAKAAwAAAAEAAgACgAAAEwCAAAEAAAAAQAAAAwAAAAIAAwABAAIAAgAAAAkAgAABAAAABUCAAB7ImluZGV4X2NvbHVtbnMiOiBbeyJraW5kIjogInJhbmdlIiwgIm5hbWUiOiBudWxsLCAic3RhcnQiOiAwLCAic3RvcCI6IDQsICJzdGVwIjogMX1dLCAiY29sdW1uX2luZGV4ZXMiOiBbeyJuYW1lIjogbnVsbCwgImZpZWxkX25hbWUiOiBudWxsLCAicGFuZGFzX3R5cGUiOiAidW5pY29kZSIsICJudW1weV90eXBlIjogIm9iamVjdCIsICJtZXRhZGF0YSI6IHsiZW5jb2RpbmciOiAiVVRGLTgifX1dLCAiY29sdW1ucyI6IFt7Im5hbWUiOiAiTmFtZSIsICJmaWVsZF9uYW1lIjogIk5hbWUiLCAicGFuZGFzX3R5cGUiOiAidW5pY29kZSIsICJudW1weV90eXBlIjogIm9iamVjdCIsICJtZXRhZGF0YSI6IG51bGx9LCB7Im5hbWUiOiAiQWdlIiwgImZpZWxkX25hbWUiOiAiQWdlIiwgInBhbmRhc190eXBlIjogImludDY0IiwgIm51bXB5X3R5cGUiOiAiaW50NjQiLCAibWV0YWRhdGEiOiBudWxsfV0sICJjcmVhdG9yIjogeyJsaWJyYXJ5IjogInB5YXJyb3ciLCAidmVyc2lvbiI6ICI4LjAuMCJ9LCAicGFuZGFzX3ZlcnNpb24iOiAiMS40LjIifQAAAAYAAABwYW5kYXMAAAIAAABMAAAABAAAAMz///8AAAECEAAAABwAAAAEAAAAAAAAAAMAAABBZ2UACAAMAAgABwAIAAAAAAAAAUAAAAAQABQACAAGAAcADAAAABAAEAAAAAAAAQUQAAAAHAAAAAQAAAAAAAAABAAAAE5hbWUAAAAABAAEAAQAAAAAAAAA/////+AAAAAUAAAAAAAAAAwAGAAGAAUACAAMAAwAAAAAAwQAHAAAAHgAAAAAAAAAAAAAAAwAHgAQAAQACAAMAAwAAACAAAAAJAAAABgAAAAEAAAAAAAAAAAAAAAAAAYACAAHAAYAAAAAAAABBQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACUAAAAAAAAAKAAAAAAAAAAhAAAAAAAAAFAAAAAAAAAAAAAAAAAAAABQAAAAAAAAACYAAAAAAAAAAAAAAAIAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAABQAAAAAAAAAKLUv/SAUoQAAAAAAAAMAAAAHAAAADAAAABAAAAAAAAAQAAAAAAAAACi1L/0gEIEAAFRvbW5pY2trcmlzaGphY2sAAAAAAAAAIAAAAAAAAAAotS/9ICCtAABwFAAVABMAEgAAAAAAAAADVAIAAwEAAP////8AAAAA\"}"}
