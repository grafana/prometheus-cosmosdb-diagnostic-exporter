package main

// Version is the build version. It is injected via -ldflags at build time
// (e.g. "-X main.Version=main-1c9dfee") and mirrors the published Docker tag.
//
// It is exported as the OpenTelemetry resource attribute `service.version`, so
// it surfaces in the `target_info{service_version=...}` metric, which lets the
// rollout-by-wave Argo workflow gate on the deployed version.
var Version = "dev"
