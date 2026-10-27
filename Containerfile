# Multi-stage build: static Go binary on ubi-micro (no shell, no package manager,
# no interpreter). The CVE surface of the final image is the binary plus the CA
# bundle.
#
# Go standard library CVEs are compiled into the binary, so the builder must run
# a Go release with the latest security fixes (verify with govulncheck, see
# README). If the UBI go-toolset lags behind, override the builder with
# --build-arg BUILDER_IMAGE=...
ARG BUILDER_IMAGE=registry.access.redhat.com/ubi9/go-toolset:latest
ARG RUNTIME_IMAGE=registry.access.redhat.com/ubi9/ubi-micro:latest

FROM ${BUILDER_IMAGE} AS build
WORKDIR /opt/app-root/src
COPY --chown=1001:0 go.mod go.sum ./
RUN go mod download && go mod verify
COPY --chown=1001:0 . .
RUN go version && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
      -o /tmp/egressip-oci-controller ./cmd

FROM ${RUNTIME_IMAGE}
LABEL name="egressip-oci-controller" \
      summary="Registers OpenShift EgressIPs as secondary private IPs on OCI VNICs"
COPY --from=build /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem \
                  /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem
COPY --from=build /tmp/egressip-oci-controller /usr/local/bin/egressip-oci-controller
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/egressip-oci-controller"]
