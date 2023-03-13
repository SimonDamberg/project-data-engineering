# !/bin/bash

# PRE: VM setup with group-19-all-machines security group, also group-19-master-sec if this is master

# Set this to change master/worker setup
is_master=false

if [ "$is_master" = "true" ];
then
  master=host-$(hostname -I | awk '{$1=$1};1' | sed 's/\./-/'g)
else
  master="host-192-168-2-144"
fi

echo "==== Installing dependencies ===="

sudo apt update -y
sudo apt install -y openjdk-11-jdk-headless curl

echo "==== Setting DNS ===="

for i in {1..4};  
do  
	for j in {1..255};  
	do  
		echo "192.168.$i.$j host-192-168-$i-$j" | sudo tee -a /etc/hosts  
	done  
done

sudo hostname host-$(hostname -I | awk '{$1=$1};1' | sed 's/\./-/'g)
echo "sudo hostname host-$(hostname -I | awk '{$1=$1};1' | sed 's/\./-/'g)" | sudo tee -a /home/ubuntu/.profile

echo "==== Installing Spark ===="

curl https://dlcdn.apache.org/spark/spark-3.2.3/spark-3.2.3-bin-hadoop3.2.tgz --output spark.tgz
tar xzvf spark.tgz
rm spark.tgz
mv spark-3.2.3-bin-hadoop3.2 spark
mkdir spark/logs

# Hadoop
echo "==== Installing Hadoop ===="

curl https://dlcdn.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz --output hadoop.tgz
tar --extract --file hadoop.tgz
rm hadoop.tgz

mv hadoop-3.3.4 hadoop
mkdir /home/ubuntu/hadoop/nameNode
mkdir /home/ubuntu/hadoop/dataNode
mkdir /home/ubuntu/hadoop/logs

echo SPARK_HOME=/home/ubuntu/spark | sudo tee -a /etc/environment
echo HADOOP_HOME=/home/ubuntu/hadoop | sudo tee -a /etc/environment
echo JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 | sudo tee -a /etc/environment

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_HOME=/home/ubuntu/spark
export HADOOP_HOME=/home/ubuntu/hadoop

echo '<configuration>

    <property>

      <name>fs.defaultFS</name>

      <value>hdfs://'$master':9000</value>

  </property>

</configuration>' | sudo tee /home/ubuntu/hadoop/etc/hadoop/core-site.xml

echo '<configuration>

<property>

  <name>dfs.namenode.name.dir</name>

  <value>/home/ubuntu/hadoop/nameNode</value>

</property>

<property>                                                               

  <name>dfs.datanode.data.dir</name>

  <value>/home/ubuntu/hadoop/dataNode</value>

</property>

<!-- 

<property>

  <name>dfs.namenode.datanode.registration.ip-hostname-check</name>

  <value>false</value>

</property>

-->

<property>

  <name>dfs.permissions.enabled</name>

  <value>false</value>

</property>

<property>

  <name>dfs.replication</name>

  <value>3</value>

</property>

<property>

  <name>dfs.namenode.rpc-bind-host</name>

  <value>0.0.0.0</value>

  <description>

    The actual address the RPC server will bind to. If this optional address is

    set, it overrides only the hostname portion of dfs.namenode.rpc-address.

    It can also be specified per name node or name service for HA/Federation.

    This is useful for making the name node listen on all interfaces by

    setting it to 0.0.0.0.

  </description>

</property>

</configuration>' | sudo tee /home/ubuntu/hadoop/etc/hadoop/hdfs-site.xml

echo "==== Starting Hadoop and Spark ===="

if [ "$is_master" = "true" ];
then
/bin/bash -c '/home/ubuntu/hadoop/bin/hdfs namenode -format'
/bin/bash -c '/home/ubuntu/hadoop/bin/hdfs --daemon start namenode'
/bin/bash -c '/home/ubuntu/spark/sbin/start-master.sh'
else
/bin/bash -c '/home/ubuntu/hadoop/bin/hdfs --daemon start datanode'
/bin/bash -c '/home/ubuntu/spark/sbin/start-worker.sh spark://192.168.2.144:7077'
fi

echo "==== Finished! ===="
