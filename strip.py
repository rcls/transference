#!/usr/bin/python3

import os
from strip_hints import strip_file_to_string

with open('transference.tmp', 'w') as out:
    print(strip_file_to_string('transference.py'), file=out)

os.chmod('transference.tmp', 0o755)
os.rename('transference.tmp', 'transference')
