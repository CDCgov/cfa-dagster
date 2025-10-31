{
  description = "hello world application using uv2nix";

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

      workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };

      overlay = workspace.mkPyprojectOverlay {
        sourcePreference = "wheel";
      };

      editableOverlay = workspace.mkEditablePyprojectOverlay {
        root = "$REPO_ROOT";
      };

      pythonSets = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          python = pkgs.python3;
        in
        (pkgs.callPackage pyproject-nix.build.packages {
          inherit python;
        }).overrideScope
          (
            lib.composeManyExtensions [
              pyproject-build-systems.overlays.wheel
              overlay
            ]
          )
      );

    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          pythonSet = pythonSets.${system}.overrideScope editableOverlay;
          virtualenv = pythonSet.mkVirtualEnv "hello-world-dev-env" workspace.deps.all;
        in
        {
          default = pkgs.mkShell {
            packages = [
              virtualenv
              pkgs.uv
            ];
            env = {
              UV_NO_SYNC = "1";
              UV_PYTHON = pythonSet.python.interpreter;
              UV_PYTHON_DOWNLOADS = "never";
            };
            shellHook = ''
              unset PYTHONPATH
              export REPO_ROOT="$(git rev-parse --show-toplevel)/infra"
            '';
          };
        }
      );

      packages = forAllSystems (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          pythonSet = pythonSets.${system}.overrideScope editableOverlay;
          virtualenv = pythonSet.mkVirtualEnv "cfa-dagster-infra" workspace.deps.all;
          dagster_home = "/opt/dagster/dagster_home";
          dagsterHomeLayer = pkgs.runCommand "dagster-home-dir" { } ''
            mkdir -p $out${dagster_home}
          '';
        in
        {
          default = pythonSets.${system}.mkVirtualEnv "cfa-dagster-infra" workspace.deps.default;
          container = pkgs.dockerTools.buildLayeredImage {
            name = "cfa-dagster-infra-nix";
            tag = "latest";
            contents = [
              virtualenv
              pkgs.uv
              pkgs.bash
              pkgs.uutils-coreutils-noprefix
              pkgs.dockerTools.usrBinEnv
              pkgs.dockerTools.binSh
              pkgs.dockerTools.caCertificates
              pkgs.dockerTools.fakeNss
              dagsterHomeLayer
            ];
            config = {
              Env = [
                "UV_NO_SYNC=1"
                "UV_PYTHON=${pythonSet.python.interpreter}"
                "UV_PYTHON_DOWNLOADS=never"
                "DAGSTER_HOME=${dagster_home}"
                "DAGSTER_USER=prod"
              ];
            };
          };
        });
    };
}
