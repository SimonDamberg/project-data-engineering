# !/bin/bash

# Preprocessing script that downloads dataset, retrieves relevant fields and saves it to HDFS
# REQUIRES: Datanode running

# Installing packages
sudo apt install -y jq

# Download dataset
echo "==== Downloading dataset ===="
wget https://files.pushshift.io/reddit/comments/RC_2010-06.zst
echo "==== Downloaded dataset! ===="

echo "==== Extracting file ===="
unzstd RC_2010-06.zst --memory=2048MB
mv RC_2010-06 RC_2010-06.json
rm RC_2010-06.zstl
echo "==== Extracted file! ===="

echo "==== Processing relevant fields ===="
jq -c '{body,score,author,subreddit_id,is_submitter}' RC_2010-06.json >> RC_2010-06_processed.json 
rm RC_2010-06.json
echo "==== Sucess! ===="

echo "Uploading data to HDFS"
/home/ubuntu/hadoop/bin/hdfs dfs -put RC_2010-06_processed.json
echo "Sucessfully uploaded!"
