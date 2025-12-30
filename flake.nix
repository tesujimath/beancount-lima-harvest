{
  description = "beancount-lima-harvest importer for Beancount/Lima";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = inputs:
    inputs.flake-utils.lib.eachDefaultSystem
      (system:
        let
          overlays = [ (import inputs.rust-overlay) ];
          pkgs = import inputs.nixpkgs {
            inherit system overlays;
          };
          # cargo-nightly based on https://github.com/oxalica/rust-overlay/issues/82
          nightly = pkgs.rust-bin.selectLatestNightlyWith (t: t.default);
          cargo-nightly = pkgs.writeShellScriptBin "cargo-nightly" ''
            export RUSTC="${nightly}/bin/rustc";
            exec "${nightly}/bin/cargo" "$@"
          '';


          ci-packages = with pkgs; [
            bashInteractive
            coreutils
            diffutils
            just

            rust-bin.stable.latest.default
            gcc

            clojure
          ];

          beancount-lima-harvest =
            let cargo = builtins.fromTOML (builtins.readFile ./rust/Cargo.toml);
            in pkgs.rustPlatform.buildRustPackage
              {
                pname = "beancount-lima-harvest";
                version = cargo.package.version;

                src = ./rust;

                cargoDeps = pkgs.rustPlatform.importCargoLock {
                  lockFile = ./rust/Cargo.lock;
                };

                meta = with pkgs.lib; {
                  description = "Beancount frontend using Lima parser";
                  homepage = "https://github.com/tesujimath/beancount-lima";
                  license = with licenses; [ asl20 mit ];
                  # maintainers = [ maintainers.tesujimath ];
                };
              };

        in
        with pkgs;
        {
          devShells.default = mkShell {
            nativeBuildInputs = [
              cargo-modules
              cargo-nightly
              cargo-udeps
              cargo-outdated
              cargo-edit
              gdb

              clojure-lsp
              neil
              jre
            ] ++ ci-packages;

            shellHook = ''
              PATH=$PATH:$(pwd)/scripts.dev:$(pwd)/rust/target/debug
            '';
          };

          packages.default = beancount-lima-harvest;

          apps = {
            tests = {
              type = "app";
              program = "${writeShellScript "beancount-lima-harvest-tests" ''
                export PATH=${pkgs.lib.makeBinPath (ci-packages ++ [beancount-lima-harvest])}
                just test
              ''}";
            };
          };
        }
      );
}
