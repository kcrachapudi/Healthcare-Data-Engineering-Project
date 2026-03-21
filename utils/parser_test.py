import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

# Import parser
from processing.x12_parser.parser import parse_x12

msg = """ISA*00*...~
NM1*IL*1*DOE*JOHN~
NM1*82*1*PROVIDER*SMITH~
CLM*12345*500~
HI*J45~"""

print(parse_x12(msg))