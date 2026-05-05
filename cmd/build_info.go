package main

// Version is the build version. It is injected via -ldflags at build time
// (e.g. "-X main.Version=main-1c9dfee") and mirrors the published Docker tag.
//
// It is exported as the OpenTelemetry resource attribute `service.version`, so
// it surfaces on `target_info` as `service_version`. Combined with the
// `k8s.cluster.name` resource attribute, this lets the rollout-by-wave Argo
// workflow gate on `target_info{service_version=..., k8s_cluster_name=...}`.
var Version = "dev"
