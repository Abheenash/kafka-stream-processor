# Native terraform tests against a mocked provider — no cluster, no cost.

mock_provider "aws" {
  override_data {
    target = data.aws_iam_policy_document.consumer_assume
    values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
  override_data {
    target = data.aws_iam_policy_document.consumer
    values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
  # Security group ids are assigned by AWS, so the rule's reference to one is
  # unknown at plan.
  override_resource {
    target          = aws_security_group.consumer
    override_during = plan
    values          = { id = "sg-0000000000000c0n" }
  }
  override_resource {
    target          = aws_security_group.cluster
    override_during = plan
    values          = { id = "sg-0000000000000c1u" }
  }
  override_resource {
    target          = aws_msk_serverless_cluster.main
    override_during = plan
    values          = { arn = "arn:aws:kafka:us-east-1:111122223333:cluster/ksp-events/abc-123" }
  }
}

variables {
  vpc_id     = "vpc-00000000000000000"
  subnet_ids = ["subnet-0000000000000000a", "subnet-0000000000000000b"]
}

run "rejects_a_single_az" {
  command = plan

  variables {
    subnet_ids = ["subnet-0000000000000000a"]
  }

  # MSK refuses to create with one AZ, and the error arrives minutes into an
  # apply. Catching it at plan time is the point of the validation.
  expect_failures = [var.subnet_ids]
}

run "the_cluster_is_reachable_only_from_the_consumer" {
  command = plan

  # A CIDR-based rule would open the port to anything that happened to be in that
  # range. Referencing the consumer's security group means only that workload can
  # reach the brokers, whatever its IP turns out to be.
  assert {
    condition     = aws_vpc_security_group_ingress_rule.kafka_tls.referenced_security_group_id != ""
    error_message = "Broker ingress must reference the consumer security group, not a CIDR block."
  }

  assert {
    condition     = aws_vpc_security_group_ingress_rule.kafka_tls.from_port == 9098
    error_message = "9098 is the SASL/IAM port. 9092 (plaintext) must never be opened."
  }
}

run "authentication_is_iam_only" {
  command = plan

  # No SASL/SCRAM password to store, rotate or leak.
  assert {
    condition     = aws_msk_serverless_cluster.main.client_authentication[0].sasl[0].iam[0].enabled
    error_message = "IAM authentication must be enabled — it is the only credential-free option."
  }
}
