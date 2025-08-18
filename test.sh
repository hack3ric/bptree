#!/bin/bash
../Mira/compiler/build/bin/cgeist -S -std=c++17 src/bptree_mira.cpp -function='*' -o bptree.mlir --sysroot=bullseye-amd64-sysroot/
../Mira/compiler/build/bin/pldisagg bptree.mlir --disagg-annotate-allocations="memory-size=10737418200" -o remote_result.mlir
../Mira/compiler/build/bin/pldisagg --prop-rmem remote_result.mlir -o target.mlir
../Mira/compiler/build/bin/pldisagg --convert-target-to-remote -cse -canonicalize target.mlir -o remote.mlir
