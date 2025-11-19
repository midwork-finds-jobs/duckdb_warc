{ pkgs, lib, config, inputs, ... }:

{
  # https://devenv.sh/packages/
  packages = [ pkgs.git pkgs.gnumake ];

  # https://devenv.sh/languages/
  languages.rust.enable = true;
  languages.python.enable = true;
}
