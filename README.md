# example ETL workflow with pyspark

Install spark
- sudo apt-get install openjdk-8-jdk
- wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2.tgz
- mkdir ~/hadoop/spark-3.3.2
- tar -xvzf spark-3.3.2-bin-hadoop3.tgz  -C ~/hadoop/spark-3.3.2 --strip 1

Bash
- code ~/.bashrc
    
    Add lines to bash
- export SPARK_HOME=~/hadoop/spark-3.0.1                                
- export PATH=$SPARK_HOME/bin:$PATH

    source  ~/.bashrc

Setup Spark default configurations
- cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf

    add lines to conf file
- code $SPARK_HOME/conf/spark-defaults.conf
- spark.driver.host	localhost

Run Spark interactive shell (http://localhost:4040)
- spark-shell

Install pyspark for python env
- pip install pyspark