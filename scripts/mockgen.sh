#!/usr/bin/env bash

set -eo pipefail

# Read the mocks.yaml file and run mockgen for each item
packages=$(yq e ".gomocks[].package" mocks.yml)
for package in $packages; do
    echo "Running mockgen for $package"

    if $(yq e ".gomocks[] | select(.package == \"${package}\") | has(\"interfaces\") " mocks.yml)
    then
       interfaces=$(yq e ".gomocks[] | select(.package == \"${package}\") | .interfaces | join(\",\")" mocks.yml)
       mockgen -destination "${package}/mocks/mocks.go" -package "${package##*/}mocks" "github.com/coinbase/chainsformer/${package}" "${interfaces}"
    fi

    if $(yq e ".gomocks[] | select(.package == \"${package}\") | has(\"sources\") " mocks.yml)
    then
      sources=$(yq e ".gomocks[] | select(.package == \"${package}\") | .sources[]" mocks.yml)
      for source in $sources; do
        sourceFile=${source##*/}
        mockgen -destination "${package}/mocks/${sourceFile%%.*}_mocks.go" -package "${package##*/}mocks" -source="${package}/${source}"
      done
    fi
done
