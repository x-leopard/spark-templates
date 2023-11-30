- Install sbt


- To compile the project into a JAR file, type:

> sbt clean package .

You can execute Spark job in the cluster with the following command:

> spark-submit --master yarn --num-executors 10--executor-cores 3--executor-memory 20g --conf spark.executor.memoryOverhead=10G \
--class org.bdf.custom.MainDVFPlusLoader dv3f_<VERSION>.jar <appName> <dataBase> <SqlFileInitial> <SqlFilesMain>


You can interactively executeinstructions inside the file sparkShellDVFPlus.scala after typing

> spark-shell --master yarn --num-executors 10--executor-cores 3--executor-memory 20g \
 --conf spark.executor.memoryOverhead=10G   --jars spark-templates_2.11-0.0.3.jar
