{ pkgs ? import <nixpkgs> {} }:

let
  # Version, URL and hash of the Spark binary
  sparkVersion = "3.5.0";
  sparkUrl = "https://archive.apache.org/dist/spark/spark-${sparkVersion}/spark-${sparkVersion}-bin-hadoop3.tgz";
  # The hash must match the official one at https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz.sha512
  sparkHash = "8883c67e0a138069e597f3e7d4edbbd5c3a565d50b28644aad02856a1ec1da7cb92b8f80454ca427118f69459ea326eaa073cf7b1a860c3b796f4b07c2101319";

  # Derivation for preparing the Spark binary
  # Warning: It does not include the JAVA runtime, it must be installed separately
  spark = pkgs.stdenv.mkDerivation {
    pname = "spark";
    version = sparkVersion;

    # Fetch the tarball
    src = pkgs.fetchurl {
      url = sparkUrl;
      sha512 = sparkHash;
    };
    # Install the tarball on the system, it will be located /nix/store/...
    installPhase = ''
      mkdir -p $out
      tar -xzf $src --strip-components=1 -C $out
    '';
    # Define the metadata of the derivation, not relevant for the build
    meta = {
      description = "Apache Spark ${sparkVersion} with prebuilt Hadoop3 binaries";
      licenses= pkgs.licenses.apache2;
      homepage = "https://spark.apache.org";
    };
  };

  # Override the sbt package to set the Java home to zulu8
  # By default the package uses OpenJDK 21
  # Check https://github.com/NixOS/nixpkgs/blob/nixos-24.11/pkgs/development/compilers/zulu/common.nix
  sbt = pkgs.sbt.overrideAttrs (oldAttrs: {
      postPatch = ''
        echo -java-home ${pkgs.zulu8.jre} >> conf/sbtopts
      '';
    });
in

# Define the development shell that includes Spark, sbt and Zulu8 JDK
pkgs.mkShell {
  packages = [
    # Install the Zulu8 JDK required for Spark and sbt
    # Packages from nixpkgs (https://search.nixos.org/packages)
    # JAVA_HOME will be set to /nix/store/.../zulu8 automatically
    pkgs.zulu8
    # sbt with a custom overlay to set the Java home
    sbt
    # Spark binary fetched from the official Apache archive
    spark
  ]; 

  # Configure the environment variables
  SPARK_HOME = "${spark.out}";
  

  # Script to be executed when the shell is started
  shellHook = ''
    echo "Your development environment for qbeast is ready, happy coding!"
    echo "Try 'spark-shell' or 'sbt test' to start."
  '';
}

