let
  # Pull in the rust-overlay (contains rust-bin)
  overlay = import (builtins.fetchTarball {
    url = "https://github.com/oxalica/rust-overlay/archive/master.tar.gz";
  });
  pkgs = import <nixpkgs> { overlays = [ overlay ]; };
in

pkgs.mkShell {
  buildInputs = [
    (pkgs.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default.override {
      extensions = [ "rust-src" "rust-analyzer-preview" "miri" ];
    }))
    pkgs.llvmPackages.clang     
    pkgs.llvmPackages.libcxx 
  ];

  RUSTFLAGS = "-Z unstable-options -C target-cpu=native";

  CC  = "${pkgs.llvmPackages.clang}/bin/clang";
  CXX = "${pkgs.llvmPackages.clang}/bin/clang++";

  shellHook = ''
    echo "Rust nightly + rust-analyzer-preview + Clang ready"
    rustc --version
    rust-analyzer --version
    which clang
    clang --version
    rustc --version
  '';
}
