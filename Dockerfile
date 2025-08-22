FROM rsantos1089/spark:scala-2.13

COPY ./kafka/jars /opt/bitnami/spark/jars
COPY ./spark/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
RUN mkdir -p /opt/bitnami/spark/spark-events