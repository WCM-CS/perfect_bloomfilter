{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    # C/C++ build tools
    clang
    llvmPackages.libclang
    pkg-config
  ];

  shellHook = ''
    export RUST_SRC_PATH="${pkgs.rustPlatform.rustcSrc}/lib/rustlib/src/rust/library"
    export LIBCLANG_PATH="${pkgs.llvmPackages.libclang.lib}/lib"
    export CC="${pkgs.clang}/bin/clang"
    export CXX="${pkgs.clang}/bin/clang++"
  '';
}
