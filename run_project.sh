#!/bin/bash

# This script is used to run the project with the specified parameters.

# ====PARAMETERS====
DATASOURCE1=input/datasource1
DATASOURCE4=input/datasource4

MR_OUTPUT=output/mr_output
FINAL_OUTPUT=output/final_output

JAR_FILE=avgfifaplayers.jar
HIVE_SCRIPT=script.hql

LOCAL_OUTPUT=result.json


# ====HDFS CLEANUP====
hadoop fs -rm -r $MR_OUTPUT
hadoop fs -rm -r $FINAL_OUTPUT

/user/kacper_grzelak456/input
# ====MAPREDUCE====
echo "Running MapReduce job..."
hadoop jar $JAR_FILE $DATASOURCE1 $MR_OUTPUT
if [ $? -ne 0 ]; then
    echo "MapReduce job failed."
    exit 1
fi

# ====HIVE====
echo "Running Hive script..."
hive \
    -f $HIVE_SCRIPT \
    -hiveconf input_dir3=$MR_OUTPUT \
    -hiveconf input_dir4=$DATASOURCE4 \
    -hiveconf output_dir=$FINAL_OUTPUT \
    -f $HIVE_SCRIPT
if [ $? -ne 0 ]; then
    echo "Hive script execution failed."
    exit 1
fi

# ====GETMERGE====
echo "merging into a single file..."
hadoop fs -getmerge $FINAL_OUTPUT $LOCAL_OUTPUT
if [ $? -ne 0 ]; then
    echo "GetMerge failed."
    exit 1
fi

# ====RESULT PRINT====
echo "Printing result..."
cat $LOCAL_OUTPUT

echo "project completed successfully."