#!/usr/bin/env python3

import re
import sys

number = 1
for line in sys.stdin:
    for match in re.findall(r"def test_\d\d_", line):
        line = line.replace(match, "def test_{:02d}_".format(number))
        number += 1
    sys.stdout.buffer.write(line.rstrip().encode("ascii") + b"\n")
