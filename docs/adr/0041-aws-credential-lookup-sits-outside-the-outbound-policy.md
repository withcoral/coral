---
status: accepted
---

# AWS credential lookup sits outside the outbound policy

Coral needs AWS credentials to read an AWS Secrets Manager Secret Store, and later for Vault's AWS IAM auth. On EC2,
ECS and EKS Pod Identity, AWS hands them out from link-local addresses, which the outbound policy never admits (ADR
0008). IRSA and an assumed role add STS calls. Coral gets credentials with AWS's own `aws-config`, using its own HTTP
client, outside `[outbound]`. Only the call that reads the secret goes through `coral-outbound` under the operator's
policy, because config sets its address.

`[outbound]` decides where Provider traffic may go. Opening link-local there would let any Provider administrator or
imported Provider document read the instance's role credentials, and through them every secret the store can read.
Credential lookup is different: the addresses are fixed by AWS or set by the operator in the environment, never by a
Provider, and the code reaches them only to get credentials.

Rejected: a link-local category in `[outbound]` (the leak above); a narrow `coral-outbound` exception for AWS's
credential addresses (keeps STS under a guard that protects nothing, and keeps us off AWS's libraries); and only
credential sources that need no link-local traffic (drops EC2, ECS and EKS Pod Identity).

The AWS identity code lives in its own core crate, outside `ee/`, so other AWS features can use it. See
#3377 for the store's settings.
