#To Delete all images
sudo docker system prune -a --volumes

#Create local copy of Spak main image
sudo docker rmi --force souvik1983/spark-engine:2.4.5
sudo docker build -t souvik1983/spark-engine:2.4.5 . --no-cache
#To enter into image
sudo docker run -it souvik1983/spark-engine:2.4.5 bash
#To push the image to Docker-hub
sudo docker push souvik1983/spark-engine:2.4.5

#Create local copy of Spak Master image
sudo docker rmi --force souvik1983/spark-master:2.4.5
sudo docker build -t souvik1983/spark-master:2.4.5 . --no-cache
sudo docker push souvik1983/spark-master:2.4.5

sudo docker run -it souvik1983/spark-master:2.4.5 bash
sudo docker run -it souvik1983/spark-worker:2.4.5 bash

#Create local copy of Spak Worker image
sudo docker rmi --force souvik1983/spark-worker:2.4.5
sudo docker build -t souvik1983/spark-worker:2.4.5 . --no-cache
sudo docker push souvik1983/spark-worker:2.4.5

kubectl exec -it spark-master-745f84854-b2tgm -- /bin/bash

#To Create Cluster
kubectl apply -f kubernets-spark-cluster.yaml
#To delete and recreate the pod
kubectl replace --force -f kubernets-spark-cluster.yaml

kubectl get pod spark-master-745f84854-lq4dk
kubectl exec -it spark-worker-8nxmh -- bash ./spark/bin/spark-shell --master spark://spark-master:7077 --conf spark.driver.host=spark-client --jars /spark/jars/guava-19.0.jar,/spark/jars/gcs-connector-hadoop3-2.0.0.jar,/spark/jars/google-cloud-storage-1.106.0.jar,/spark/jars/spark-bigquery-latest.jar --files /opt/hadoop/conf/core-site.xml

#helm install incubator/sparkoperator --generate-name --skip-crds

kubectl rollout restart deploy spark-master --image=souvik1983/spark-master:2.4.5 --image-pull-policy Always

#To expose Load Balancer
kubectl expose deployment spark-master --port=8080 --target-port=8080 --name=spark-master-load-balancer --type=LoadBalancer

#To execute Spark-Shell
kubectl run spark-engine --rm -it --labels="app=spark-client" --image souvik1983/spark-engine:2.4.5 -- bash ./spark/bin/spark-shell --master spark://spark-master:7077 --conf spark.driver.host=spark-client --properties=spark.driver.extraJavaOptions="-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:+UseG1GC  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp" 


--jars https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery_2.11/0.14.0-beta/spark-bigquery_2.11-0.14.0-beta.jar

#https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest_2.12.jar

--conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"


--jars /spark/jars/guava-14.0.1.jar,https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-2.0.1.jar,/spark/jars/spark-bigquery-latest.jar --files /opt/hadoop/conf/core-site.xml

/spark/jars/gcs-connector-latest-hadoop2.jar

curl -i -X DELETE \
  -H "Accept: application/json" \
  -H "Authorization: JWT $HUB_TOKEN" \
  https://hub.docker.com/v2/repositories/souvik1983/spark-engine:2.4.5/

"ls /opt/hadoop/conf/" !

export SPARK_CLASSPATH=$SPARK_CLASSPATH:/spark/jars/guava-19.0.jar
export SPARK_CLASSPATH=$SPARK_CLASSPATH:/spark/jars/gcs-connector-hadoop3-2.0.0.jar
export SPARK_CLASSPATH=$SPARK_CLASSPATH:/spark/jars/google-cloud-storage-1.106.0.jar
export SPARK_CLASSPATH=$SPARK_CLASSPATH:/spark/jars/spark-bigquery-latest.jar

kubectl run spark-engine --rm -it --labels="app=spark-client" --image souvik1983/spark-engine:2.4.5 -- bash ./spark/bin/spark-shell --master spark://spark-master:7077  --conf spark.driver.host=spark-client

#IMPORTANT
https://github.com/GoogleCloudPlatform/spark-on-k8s-operator

#To Execute Spark-Submit
kubectl run spark-engine --rm -it --labels="app=spark-client" --image souvik1983/spark-engine:2.4.5 -- bash ./spark/bin/spark-submit --class CLASS_TO_RUN --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-client URL_TO_YOUR_APP

gs://souvik_storage/sales_data_sample.csv

#To delete pod
kubectl delete pods spark-test-pod

kubectl port-forward pod/spark-master-745f84854-qvplk 8080:8080

#To add load balancer
kubectl apply -f https://k8s.io/examples/service/load-balancer-example.yaml
kubectl get deployments hello-world
kubectl describe deployments hello-world
kubectl get replicasets
kubectl describe replicasets
kubectl expose deployment hello-world --type=LoadBalancer --name=my-service
kubectl get services my-service
kubectl describe services my-service

#To cleanup Loadbalancer Service
kubectl delete services my-service
kubectl delete deployment hello-world

gcloud config set disable_usage_reporting false

