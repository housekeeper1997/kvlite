#! /usr/bin/env python3

import os
import hashlib

def succeed(desc:str):
    return op_result('succeed', desc)

def failed(desc:str):
    return op_result('failed', desc)

def op_result(result: str, desc: str):
    return {"result":result, "desc":desc}

def getByteLen(value: str):
    return len(value.encode('utf-8'))

def strExceedLimit(s: str, limit: int):
    byteLen = getByteLen(s)
    return byteLen > limit

def env_str(key: str, defaultValue: str):
    return os.getenv(key, defaultValue)

def env_type(key: str, defaultValue, type):
    value = os.getenv(key, str(defaultValue))
    return type(value)

def env_int(key: str, defaultValue: int):
    return env_type(key, defaultValue, int)

def env_float(key: str, defaultValue: float):
    return env_type(key, defaultValue, float)

def sha256(text: str):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()
