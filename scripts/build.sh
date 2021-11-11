#!/bin/bash

# SPDX-License-Identifier: Apache-2.0

docker tag hyperledger/fabric-tools harrymknight/fabric-tools:debug
docker tag hyperledger/fabric-orderer harrymknight/fabric-orderer:debug
docker tag hyperledger/fabric-peer harrymknight/fabric-peer:debug
docker push harrymknight/fabric-tools:debug
docker push harrymknight/fabric-orderer:debug
docker push harrymknight/fabric-peer:debug
echo "Pushed images"