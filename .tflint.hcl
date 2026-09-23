# tflint catches what `terraform validate` cannot: deprecated syntax, unused
# declarations, and naming drift.
plugin "terraform" {
  enabled = true
  preset  = "recommended"
}

plugin "aws" {
  enabled = true
  version = "0.44.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}

rule "terraform_required_version" { enabled = true }
rule "terraform_required_providers" { enabled = true }
rule "terraform_unused_declarations" { enabled = true }
rule "terraform_naming_convention" {
  enabled = true
  format  = "snake_case"
}
