#!/bin/bash

set -e
mira_compiler_bin="../Mira/compiler/build/bin"
mira_llvm_bin="../Mira/llvm-project/build/bin"

rm *.mlir *.o *.ll || :

"$mira_compiler_bin"/cgeist -S -std=c++17 --emit-llvm-dialect -I include src/bptree_mira.cpp -function='*' -o bptree_lower_ref.mlir --sysroot=focal-amd64-sysroot/
"$mira_llvm_bin"/mlir-translate --mlir-to-llvmir bptree_lower_ref.mlir -o lower_ref.ll
"$mira_llvm_bin"/clang-16 -O3 -c lower_ref.ll -o lower_ref.o

"$mira_compiler_bin"/cgeist -S -std=c++17 -I include src/bptree_mira.cpp -function='*' -o bptree.mlir --sysroot=focal-amd64-sysroot/
"$mira_compiler_bin"/pldisagg bptree.mlir --disagg-annotate-allocations="memory-size=107374182" -o remote_result.mlir
"$mira_compiler_bin"/pldisagg --prop-rmem remote_result.mlir -o target.mlir
"$mira_compiler_bin"/pldisagg --convert-target-to-remote -cse -canonicalize target.mlir -o remote.mlir

"$mira_compiler_bin"/pldisagg remote.mlir --rmem-loop-normal-cache="ccfg=cache.1.cfg dist=1" -cse --canonicalize -o pref.mlir
"$mira_compiler_bin"/pldisagg --emit-llvm="ccfg=cache.1.cfg" -reconcile-unrealized-casts -cse --canonicalize -cse pref.mlir -o lower.mlir

"$mira_llvm_bin"/mlir-translate --mlir-to-llvmir lower.mlir -o lower.ll
"$mira_llvm_bin"/clang-16 -O3 -c lower.ll -o lower.o

# This does not work with std::vector<bptree_node_t> but okay with std::vector<int>;
