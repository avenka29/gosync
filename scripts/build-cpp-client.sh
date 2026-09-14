#!/usr/bin/env bash
set -euo pipefail
project_dir=$(cd "$(dirname "$0")/.." && pwd)
work=${1:?Pass a temporary build directory}
mkdir -p "$work"
git clone --quiet --recurse-submodules https://github.com/socketio/socket.io-client-cpp.git "$work/source"
git -C "$work/source" checkout --quiet 3b7be7e4173b5bdeed393966e3274f65d513a280
git -C "$work/source" submodule update --init --recursive
git -C "$work/source" apply --unidiff-zero "$project_dir/integration/cpp-engineio4.patch"
cmake -S "$work/source" -B "$work/build" -DBUILD_TESTING=OFF -DDISABLE_LOGGING=ON -DCMAKE_EXPORT_NO_PACKAGE_REGISTRY=ON
cmake --build "$work/build" --target sioclient -j 4
"${CXX:-c++}" -std=c++11 -I "$work/source/src" "$project_dir/integration/cpp-client.cpp" "$work/build/libsioclient.a" -pthread -o "$work/cpp-check"
printf 'C++ client executable: %s/cpp-check\n' "$work"
