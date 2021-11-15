#!/bin/bash

# SPDX-License-Identifier: Apache-2.0

docker tag hyperledger/fabric-tools harrymknight/fabric-tools:2.3.3
docker tag hyperledger/fabric-orderer harrymknight/fabric-orderer:2.3.3
docker tag hyperledger/fabric-peer harrymknight/fabric-peer:2.3.3
docker push harrymknight/fabric-tools:2.3.3
docker push harrymknight/fabric-orderer:2.3.3
docker push harrymknight/fabric-peer:2.3.3
echo "Pushed images"