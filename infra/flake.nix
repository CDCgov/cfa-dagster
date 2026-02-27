{
  description = "CFA Dagster infra container image";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";

    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      nixpkgs,
      pyproject-nix,
      uv2nix,
      pyproject-build-systems,
      ...
    }:
    let
      inherit (nixpkgs) lib;
      forAllSystems = lib.genAttrs lib.systems.flakeExposed;

      # Load the workspace from the infra directory
      workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };

      # Create overlay for dependencies
      overlay = workspace.mkPyprojectOverlay {
        sourcePreference = "wheel";
      };

      # Get python packages for a system
      pythonPackages =
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          python = pkgs.python313;
        in
        (pkgs.callPackage pyproject-nix.build.packages {
          inherit python;
        }).overrideScope
          (
            lib.composeManyExtensions [
              pyproject-build-systems.overlays.wheel
              overlay
            ]
          );
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          pythonPkgs = pythonPackages system;

          # Create virtualenv with all dependencies
          virtualenv = pythonPkgs.mkVirtualEnv "cfa-dagster-env" workspace.deps.all;

          dagsterHome = "/opt/dagster/dagster_home";

          # Create DAGSTER_HOME directory
          dagsterHomeLayer = pkgs.runCommand "dagster-home-dir" { } ''
            mkdir -p $out${dagsterHome}
            # uncomment to include dagster_defs.py in the final image
            # cp ${./dagster_defs.py} $out${dagsterHome}/dagster_defs.py
          '';

          # Create a runtime environment with all necessary files
          runtimeEnv = pkgs.buildEnv {
            name = "cfa-dagster-runtime";
            paths = [
              # Python virtualenv with all dependencies
              virtualenv

              # Runtime dependencies
              pkgs.glibc
              pkgs.gcc.cc.lib
              pkgs.stdenv.cc.cc
              pkgs.binutils
              pkgs.bash
              pkgs.coreutils
              pkgs.busybox

              # SSL certificates for network requests
              pkgs.dockerTools.caCertificates

              # Environment utilities
              pkgs.dockerTools.usrBinEnv
              pkgs.dockerTools.binSh
              pkgs.neovim
              pkgs.uv

              # DAGSTER_HOME directory
              dagsterHomeLayer
            ];
          };

          # Build the container image using dockerTools without a base image
          container = pkgs.dockerTools.buildImage {
            name = "cfa-dagster-infra";
            tag = "nix";

            copyToRoot = runtimeEnv;

            # Image configuration
            config = {
              Env = [
                "PATH=/bin:/usr/bin:${virtualenv}/bin"
                "DAGSTER_HOME=${dagsterHome}"
                "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
              ];

              WorkingDir = dagsterHome;

              # Default command
              Cmd = [ "python" "-c" "import cfa_dagster" ];
            };
          };
        in
        {
          default = container;
          container = container;

          # Also provide the virtualenv as a separate package
          venv = virtualenv;
        }
      );

      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          pythonPkgs = pythonPackages system;
          virtualenv = pythonPkgs.mkVirtualEnv "cfa-dagster-dev" workspace.deps.all;
        in
        {
          default = pkgs.mkShell {
            packages = [
              virtualenv
              pkgs.uv
              pkgs.python313
            ];

            env = {
              UV_NO_SYNC = "1";
              UV_PYTHON = "3.13";
              UV_PYTHON_DOWNLOADS = "never";
            };

            shellHook = ''
              unset PYTHONPATH
            '';
          };
        }
      );
    };
}
