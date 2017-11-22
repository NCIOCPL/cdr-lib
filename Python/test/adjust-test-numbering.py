import re
import sys

number = 1
for line in sys.stdin:
    for match in re.findall(r"def test_\d\d_", line):
        line = line.replace(match, "def test_{:02d}_".format(number))
        number += 1
    print(line.rstrip())
