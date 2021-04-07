export CLUSTER_NAME="<ADD_CLUSTER_NAME>r"

gcloud beta dataproc clusters create ${CLUSTER_NAME} \
     --worker-machine-type n1-standard-4 \
     --num-workers 0 \
     --image-version 1.5-debian \
     --initialization-actions gs://dataproc-initialization-actions/python/pip-install.sh \
     --metadata 'PIP_PACKAGES=google-cloud-storage' \
     --optional-components=ANACONDA \
     --enable-component-gateway


gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    --jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --driver-log-levels root=FATAL \
    /Users/velascoluis/PycharmProjects/dagster_gpu/src/spark/analyzeStackOverflow.py