# MSK Serverless: no broker count, no instance type, no storage to size. It bills
# per GB-in/out and per partition-hour, which suits a workload that is bursty and
# occasionally idle — the same reasoning as DynamoDB on-demand elsewhere in these
# projects.
#
# NOT APPLIED. MSK Serverless bills per partition-hour whether or not anything is
# flowing, so a cluster left up quietly costs money. This is written, validated
# and tested; the processor's behaviour is proven against a fake broker instead.

resource "aws_security_group" "cluster" {
  name_prefix = "${var.name_prefix}-msk-"
  description = "MSK Serverless cluster"
  vpc_id      = var.vpc_id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "kafka_tls" {
  security_group_id            = aws_security_group.cluster.id
  description                  = "Kafka TLS from the consumer security group only"
  from_port                    = 9098
  to_port                      = 9098
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.consumer.id
}

resource "aws_security_group" "consumer" {
  name_prefix = "${var.name_prefix}-consumer-"
  description = "Stream processor"
  vpc_id      = var.vpc_id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_egress_rule" "consumer_to_kafka" {
  security_group_id            = aws_security_group.consumer.id
  description                  = "To the MSK cluster"
  from_port                    = 9098
  to_port                      = 9098
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.cluster.id
}

resource "aws_msk_serverless_cluster" "main" {
  cluster_name = "${var.name_prefix}-events"

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.cluster.id]
  }

  # IAM auth is the only option on Serverless, and it is the right one: no SASL
  # password to store, rotate or leak. Authorisation is an IAM policy, so a
  # consumer can be granted exactly one topic and one consumer group.
  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}

data "aws_iam_policy_document" "consumer_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "consumer" {
  name               = "${var.name_prefix}-consumer"
  assume_role_policy = data.aws_iam_policy_document.consumer_assume.json
}

# Least privilege on Kafka is finer-grained than most people use it. The consumer
# can read ONE topic, write ONE dead-letter topic, and join ONE group — it cannot
# create topics, describe the cluster's other topics, or join another group.
data "aws_iam_policy_document" "consumer" {
  statement {
    sid       = "Connect"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
    resources = [aws_msk_serverless_cluster.main.arn]
  }

  statement {
    sid     = "ReadTheEventsTopic"
    actions = ["kafka-cluster:DescribeTopic", "kafka-cluster:ReadData"]
    resources = [
      "${replace(aws_msk_serverless_cluster.main.arn, ":cluster/", ":topic/")}/events",
    ]
  }

  statement {
    sid     = "WriteOnlyToTheDlq"
    actions = ["kafka-cluster:DescribeTopic", "kafka-cluster:WriteData"]
    resources = [
      "${replace(aws_msk_serverless_cluster.main.arn, ":cluster/", ":topic/")}/events.dlq",
    ]
  }

  statement {
    sid     = "OneConsumerGroup"
    actions = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
    resources = [
      "${replace(aws_msk_serverless_cluster.main.arn, ":cluster/", ":group/")}/${var.name_prefix}-processor",
    ]
  }
}

resource "aws_iam_role_policy" "consumer" {
  name   = "${var.name_prefix}-consumer-kafka"
  role   = aws_iam_role.consumer.id
  policy = data.aws_iam_policy_document.consumer.json
}
