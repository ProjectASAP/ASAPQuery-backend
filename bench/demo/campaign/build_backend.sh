#!/bin/bash
# build_backend.sh <sha> [hosts...]: build data_plane/control_plane at <sha> on $BUILD_HOST (own dirs), run the
# parity test, and install into $BENCH_ROOT/campaign-bin-<sha> on the given hosts (default: $BENCH_HOSTS, the engine hosts and
# the demo drivers). 2026-11-23: never writes into an existing directory (another job may be using it):
# a host that already has $BENCH_ROOT/campaign-bin-<sha> is skipped and reported; a new directory is filled
# in a temp dir and renamed into place. It no longer copies $BIN_DIR into the directory.
set -e
sha=$1; shift; hosts=${*:-${BENCH_HOSTS:?set BENCH_HOSTS}}
BUILD=${BUILD_HOST:?set BUILD_HOST}
git -C $BENCH_ROOT/asapq-backend-g4 fetch -q origin
git -C $BENCH_ROOT/asapq-backend-g4 archive $sha | ssh $BUILD "rm -rf $BENCH_ROOT/backend-campaign-$sha && mkdir -p $BENCH_ROOT/backend-campaign-$sha && tar -x -C $BENCH_ROOT/backend-campaign-$sha"
ssh $BUILD "source $BENCH_ENV; cd $BENCH_ROOT/backend-campaign-$sha && export CARGO_TARGET_DIR=$BENCH_ROOT/target-campaign && cargo build --release -p data_plane -p control_plane > $BENCH_ROOT/tmp/campaign-build-$sha.log 2>&1 && rm -rf $BENCH_ROOT/campaign-stage-$sha && mkdir -p $BENCH_ROOT/campaign-stage-$sha && cp \$CARGO_TARGET_DIR/release/data_plane \$CARGO_TARGET_DIR/release/control_plane $BENCH_ROOT/campaign-stage-$sha/ && echo $sha > $BENCH_ROOT/campaign-stage-$sha/BACKEND_COMMIT && cargo test --release -p data_plane --test planner_executor_parity > $BENCH_ROOT/tmp/campaign-parity-$sha.log 2>&1; echo parity rc=\$?; grep -E '^test |test result' $BENCH_ROOT/tmp/campaign-parity-$sha.log"
S=$BENCH_ROOT/tmp/campaign-stage/bin-$sha; mkdir -p $S; rsync -a $BUILD:$BENCH_ROOT/campaign-stage-$sha/ $S/
for h in $hosts; do
  ( H=$h
    if ssh $H "test -e $BENCH_ROOT/campaign-bin-$sha"; then echo "$h: $BENCH_ROOT/campaign-bin-$sha exists, left untouched ($(ssh $H cat $BENCH_ROOT/campaign-bin-$sha/BACKEND_COMMIT 2>/dev/null))"; exit 0; fi
    ssh $H "rm -rf $BENCH_ROOT/.campaign-bin-$sha.tmp && mkdir -p $BENCH_ROOT/.campaign-bin-$sha.tmp" &&
    rsync -a $S/ $H:$BENCH_ROOT/.campaign-bin-$sha.tmp/ &&
    ssh $H "cd $BENCH_ROOT/.campaign-bin-$sha.tmp && for f in victoria-metrics-v1.156.0 vmstorage-v1.156.0 vmselect-v1.156.0 vminsert-v1.156.0 paced_feeder_realistic; do for src in $BENCH_ROOT/campaign-bin-8368fcfd/\$f $BIN_DIR/\$f; do [ -e \$src ] && { cp -a \$src .; break; }; done; done; mv $BENCH_ROOT/.campaign-bin-$sha.tmp $BENCH_ROOT/campaign-bin-$sha && echo $h \$(cat $BENCH_ROOT/campaign-bin-$sha/BACKEND_COMMIT)" ) &
done; wait
