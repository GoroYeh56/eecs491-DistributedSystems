# run go test -run TestXX -count COUNT time > OUT file
# $1 : TestCase Name
# $2 : Number of time we want to run go test
# $3 : Output file path

#!/bin/sh
if [ "$#" -ne 3 ]; then
  echo "Usage: ./go_test_caen.sh TEST_NAME TEST_COUNT OUT_FILE_NAME "
  exit 1
fi

echo "Test: $1"
echo "Test Iteration: $2"
echo "Output Filename: $3"

DIRECTORY="caen"

# Use a for-loop?
if [ -d $DIRECTORY ]; then
    echo "$DIRECTORY does exist."
    rm -r $DIRECTORY
else
    echo "$DIRECTORY doesn't exist.\nCreate a new one..."
fi

# if [ $PWD != "/home/goroyeh/goroyeh.5/shardkv" ]
# then
#    echo "Wrong Path! Please go to project5/shardkv/ to execute this script!" 
#    exit
# fi

mkdir $DIRECTORY
it=0
while [ "$it" -lt $2 ]    # this is loop1
do
   go test -run $1 > "caen/$3-$it" &
  #  echo it=`expr $it + 1`
done