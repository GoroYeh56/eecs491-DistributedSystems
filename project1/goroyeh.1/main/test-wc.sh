#!/bin/bash
rm -f diff.out output/*
go run main.go master kjv12.txt sequential
sort -k2n -k1 mr-testout.txt > mr-testout.sorted
sort -k2n -k1 output/mrtmp.kjv12.txt | tail -1005 | diff - mr-testout.sorted > diff.out
if [ -s diff.out ]
then
echo "Failed test. Output should be as in mr-testout.sorted. Your output differs as follows (from diff.out):"
  # cat diff.out
else
  echo "Passed test"
fi
