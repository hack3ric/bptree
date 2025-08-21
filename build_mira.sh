#!/bin/bash

mira_compiler_bin="../Mira/compiler/build/bin"

"$mira_compiler_bin"/cgeist -S -std=c++17 src/bptree_mira.cpp -function='*' -o bptree.mlir --sysroot=focal-amd64-sysroot/
"$mira_compiler_bin"/pldisagg bptree.mlir --disagg-annotate-allocations="memory-size=10737418200" -o remote_result.mlir
"$mira_compiler_bin"/pldisagg --prop-rmem remote_result.mlir -o target.mlir
"$mira_compiler_bin"/pldisagg --convert-target-to-remote -cse -canonicalize target.mlir -o remote.mlir

# This does not work with std::vector<bptree_node_t> but okay with std::vector<int>;
