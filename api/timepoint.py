#! /usr/bin/env python3

from time import time

class timepoint():
    def __init__(self, t: float = None) -> None:
        self.time = t if t is not None else time()

    def set(self, point: float=None):
        if point is None:
            point = time()
        self.time = point

    def elapsed(self) -> float:
        return time() - self.time
