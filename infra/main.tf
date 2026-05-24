# =================================================================
# FraudShield - Infraestructura como Código (Terraform)
# Simulación de aprovisionamiento de recursos para la arquitectura Kappa
# =================================================================
 
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
 
provider "aws" {
  region = "us-east-1"
}
 
# 1. Almacenamiento de Objetos (El cimiento del Lakehouse)
resource "aws_s3_bucket" "fraudshield_lakehouse" {
  bucket = "fraudshield-data-lake-2026"
  tags = {
    Environment = "Produccion"
    Layer       = "Almacenamiento"
  }
}
 
# 2. Clúster de Apache Kafka (MSK - Managed Streaming for Apache Kafka)
resource "aws_msk_cluster" "kafka_ingesta" {
  cluster_name           = "fraudshield-kafka-cluster"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 3 # Tolerancia a fallos
 
  broker_node_group_info {
    instance_type = "kafka.m5.large"
    storage_info {
      ebs_storage_info {
        volume_size = 1000
      }
    }
    client_subnets = ["subnet-xyz1", "subnet-xyz2", "subnet-xyz3"]
  }
}
 
# 3. Clúster de Apache Spark (EMR - Elastic MapReduce)
resource "aws_emr_cluster" "spark_procesamiento" {
  name          = "fraudshield-spark-streaming"
  release_label = "emr-7.0.0"
  applications  = ["Spark", "Hadoop"]
 
  master_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = 1
  }
 
  core_instance_group {
    instance_type  = "r5.xlarge" # Optimizadas para memoria (estado de ventanas)
    instance_count = 8           # Escalable hasta 32 ejecutores
  }
}