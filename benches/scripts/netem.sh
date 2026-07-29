#!/bin/sh
# netem.sh — apply a high-RTT emulation profile to the shared Kafka broker
# container (issue #52 B2).
#
# Mechanism: a short-lived sidecar container with CAP_NET_ADMIN joins the
# broker container's network namespace and drives `tc netem` on the broker's
# eth0 egress. The broker image itself (apache/kafka) ships no `tc`, and this
# works on any Linux docker host (colima VM, CI runner) without touching
# host-side veth devices. Delaying broker egress only delays every
# response/outbound packet by the full profile value, so each request/response
# round trip gains ~= the profile's delay — i.e. the profile value IS the
# added RTT (asymmetric placement, symmetric effect on RTT).
#
# Usage:
#   benches/scripts/netem.sh <lan|wan50|wan150|wan200-lossy|off|status>
#
# Profiles:
#   lan | off     — remove shaping entirely (restore default qdisc)
#   wan50         — +50ms RTT, ±5ms jitter
#   wan150        — +150ms RTT, ±15ms jitter, 0.1% loss
#   wan200-lossy  — +200ms RTT, ±20ms jitter, 1% loss
#   status        — show the broker's current qdisc
#
# Idempotent: profiles use `tc qdisc replace`, so re-applying a profile (or a
# different one) replaces the previous shaping; `off` fully cleans up and is
# safe to run when nothing is applied.
#
# ALWAYS run `netem.sh off` when done — the target broker is shared.
#
# Environment:
#   KACRAB_NETEM_TARGET — broker container name (default kacrab-kafka)
#   KACRAB_NETEM_DEV    — interface inside the container netns (default eth0)
#   KACRAB_NETEM_IMAGE  — sidecar image tag to build/use (default kacrab-netem)

set -eu

TARGET=${KACRAB_NETEM_TARGET:-kacrab-kafka}
DEV=${KACRAB_NETEM_DEV:-eth0}
IMAGE=${KACRAB_NETEM_IMAGE:-kacrab-netem}

# netem's default queue limit (1000 packets) turns into an accidental
# bandwidth cap at high bandwidth-delay products; raise it so the profiles
# shape latency/loss, not throughput.
LIMIT=100000

usage() {
    echo "usage: $0 <lan|wan50|wan150|wan200-lossy|off|status>" >&2
    exit 2
}

[ $# -eq 1 ] || usage
PROFILE=$1

ensure_image() {
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        echo "netem: building sidecar image '$IMAGE' (alpine + iproute2-tc)" >&2
        docker build -q -t "$IMAGE" - >/dev/null <<'EOF'
FROM alpine:3
RUN apk add --no-cache iproute2-tc
EOF
    fi
}

# Run `tc` inside the broker container's network namespace.
tc_in_netns() {
    docker run --rm --net="container:${TARGET}" --cap-add NET_ADMIN \
        "$IMAGE" tc "$@"
}

apply() {
    ensure_image
    # shellcheck disable=SC2086 # $1 is a word list built by this script
    tc_in_netns qdisc replace dev "$DEV" root netem limit "$LIMIT" $1
    echo "netem: applied '$PROFILE' to ${TARGET}:${DEV}"
    tc_in_netns qdisc show dev "$DEV"
}

case $PROFILE in
lan | off)
    ensure_image
    # `del root` fails when no qdisc is installed; that is the clean state.
    tc_in_netns qdisc del dev "$DEV" root 2>/dev/null ||
        echo "netem: ${TARGET}:${DEV} already clean" >&2
    echo "netem: shaping removed from ${TARGET}:${DEV}"
    tc_in_netns qdisc show dev "$DEV"
    ;;
wan50)
    apply "delay 50ms 5ms"
    ;;
wan150)
    apply "delay 150ms 15ms loss 0.1%"
    ;;
wan200-lossy)
    apply "delay 200ms 20ms loss 1%"
    ;;
status)
    ensure_image
    tc_in_netns qdisc show dev "$DEV"
    ;;
*)
    usage
    ;;
esac
