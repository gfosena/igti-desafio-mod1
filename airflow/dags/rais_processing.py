from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import boto3
from airflow.models import Variable

aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")


# coloque sua região abaixo

client = boto3.client("emr", region_name="sa-east-1",
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)

s3client = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)


# verificar parâmetro 'owner' abaixo, adaptar se necessário

# Usando a novíssima Taskflow API
default_args = {
    'owner': 'gustavo sena',
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False
}

@dag(default_args=default_args, schedule_interval=None, catchup=False, tags=["emr", "aws", "enem"], description="Pipeline para processamento de dados do ENEM 2019")
def pipeline_rais():
    """
    Pipeline para processamento de dados da rais 2020 
    """

    @task
    def emr_process_rais_data():
        cluster_id = client.run_job_flow(
            # verificar nome do cluster e adaptar, se necessário
            Name='EMR-gustavo-IGTI',
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
            VisibleToAllUsers=True,
            # verificar endereço s3 e adaptar, se necessário
            LogUri='s3://datalake-gustavo-igti-edc-tf/emr-logs',
            ReleaseLabel='emr-6.3.0',
            # ReleaseLabel='emr-6.5.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'SPOT',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Worker nodes',
                        'Market': 'SPOT',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    }
                ],
                # verificar Ec2KeyName e adaptar, se necessário
                'Ec2KeyName': 'gustavo-igti-teste',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                # adaptar sua subnet
                'Ec2SubnetId': 'subnet-00f2b6c87578c1bc5'
            },

            Applications=[{'Name': 'Spark'}],

            Configurations=[{
                "Classification": "spark-env",
                "Properties": {},
                "Configurations": [{
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3",
                        "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                    }
                }]
            },
                {
                    "Classification": "spark-hive-site",
                    "Properties": {
                        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    }
                },
                {
                    "Classification": "spark-defaults",
                    "Properties": {
                        "spark.submit.deployMode": "cluster",
                        "spark.speculation": "false",
                        "spark.sql.adaptive.enabled": "true",
                        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                    }
                },
                {
                    "Classification": "spark",
                    "Properties": {
                        "maximizeResourceAllocation": "true"
                    }
                }
            ],

            Steps=[{
                'Name': 'processamento dos dados da rais',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit',
                            '--packages', 'io.delta:delta-core_2.12:1.0.0',
                            # '--packages', 'io.delta:delta-core_2.12:1.1.0', 
                            '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension', 
                            '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog', 
                            '--master', 'yarn',
                            '--deploy-mode', 'cluster',
                            # verificar endereço s3 e adaptar, se necessário
                            's3://dl-gustavo-igti-edc-desafio-mod1/emr-code/pyspark/job_spark_rais.py'
                        ]
                }
            }],
        )
        return cluster_id["JobFlowId"]


    @task
    def wait_emr_step(cid: str, stepId: str):
        waiter = client.get_waiter('step_complete')
      
        waiter.wait(
            ClusterId=cid,
            StepId=stepId,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 120
            }
        )
        return True

   
    @task
    def terminate_emr_cluster(success_before: str, cid: str):
        if success_before:
            res = client.terminate_job_flows(
                JobFlowIds=[cid]
            )


 
 # Encadeando a pipeline
    cluid = emr_process_rais_data()
    res_emr = wait_emr_step(cluid)
    # newstep = upsert_delta(cluid, res_emr)
    # res_ba = wait_upsert_delta(cluid, newstep)
    # res_ter = terminate_emr_cluster(res_ba, cluid)
    res_ter = terminate_emr_cluster(res_emr, cluid)

execucao = pipeline_rais()