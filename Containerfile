# FIPS-capable build. The Red Hat Go toolchain routes crypto through the RHEL
# OpenSSL library when built with CGO_ENABLED=1; on a host with FIPS mode
# enabled, OpenSSL runs with its FIPS provider. strictfipsruntime makes the
# binary exit at startup instead of falling back to Go crypto when FIPS mode is
# enabled but OpenSSL cannot be loaded.
#
# The runtime image is ubi-micro plus openssl-libs and its dependencies
# (FIPS provider, crypto-policies, CA bundle); it has no package manager.
#
# Go standard library CVEs are compiled into the binary. Red Hat backports
# fixes without changing the Go version, so govulncheck results for this
# toolchain must be checked against the Red Hat CVE database (see README).
ARG BUILDER_IMAGE=registry.access.redhat.com/ubi9/go-toolset:latest
ARG PACKAGER_IMAGE=registry.access.redhat.com/ubi9/ubi:latest
ARG RUNTIME_IMAGE=registry.access.redhat.com/ubi9/ubi-micro:latest

FROM ${BUILDER_IMAGE} AS build
WORKDIR /opt/app-root/src
COPY --chown=1001:0 go.mod go.sum ./
RUN go mod download && go mod verify
COPY --chown=1001:0 . .
RUN go version && \
    CGO_ENABLED=1 GOEXPERIMENT=strictfipsruntime \
      go build -tags strictfipsruntime -trimpath -ldflags="-s -w" \
      -o /tmp/egressip-oci-controller ./cmd

FROM ${RUNTIME_IMAGE} AS runtime-base

FROM ${PACKAGER_IMAGE} AS rootfs
COPY --from=runtime-base / /rootfs
RUN dnf install -y --installroot /rootfs --releasever 9 \
      --setopt install_weak_deps=false --nodocs openssl-libs && \
    dnf clean all --installroot /rootfs && \
    rm -rf /rootfs/var/cache/* /rootfs/var/log/dnf* /rootfs/var/log/yum.*

FROM scratch
LABEL name="egressip-oci-controller" \
      summary="Registers OpenShift EgressIPs as secondary private IPs on OCI VNICs"
COPY --from=rootfs /rootfs /
COPY --from=build /tmp/egressip-oci-controller /usr/local/bin/egressip-oci-controller
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/egressip-oci-controller"]
