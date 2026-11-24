---
status: accepted
---

# The Enterprise Install ships as one Helm chart, versioned with Coral

An Enterprise Install runs Coral on a customer's Kubernetes. We ship it as one plain Helm chart and nothing else: no
operator, no Replicated/KOTS and no Terraform module. The chart lives in this repo and shares Coral's version, so chart
`1.10.0` installs Coral `1.10.0`. The release that tags the images also publishes the chart, as a public, cosign-signed
OCI artifact on GHCR, with no HTTP chart repo. It replaces the hand-written Kubernetes manifests; people who don't use
Helm render YAML with `helm template`.

A plain chart is what enterprise teams expect and what GitOps tools such as Argo CD and Flux consume. Operators are rare
among comparable projects, and none of those we surveyed uses Replicated. Keeping the chart next to the code lets a
change to a listener, setting or migration change the chart in the same commit.

Sharing Coral's version means customers never need a chart-to-Coral matrix, and breaking chart releases were the top
complaint we found about other projects' charts. The cost is that a fix to the chart alone ships as a Coral patch
release. We accept that.

The chart is public because the License, checked offline, decides what an Install may do. A gated registry would add
pull secrets and mirroring work and protect nothing.

The survey behind this is withcoral/coral-cloud#473. The decision is withcoral/coral-cloud#476.
