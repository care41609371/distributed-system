#!/usr/bin/env python

while True:
    s = input()
    if len(s) == 0:
        break
    if s[: 4] == "2022":
        print(s)
