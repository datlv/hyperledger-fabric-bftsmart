/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"github.com/hyperledger/fabric/common/tools/configtxgen/encoder"
	genesisconfig "github.com/hyperledger/fabric/common/tools/configtxgen/localconfig"
	cb "github.com/hyperledger/fabric/protos/common"
)

func newChainRequest(consensusType, creationPolicy, newChannelId string) *cb.Envelope {

	//JCS: added application organizations to avoid the config transaction from being rejected
	orgs := genConf.Application.Organizations

	var orgNames []string
	for _, org := range orgs {
		orgNames = append(orgNames, org.Name)
	}

	env, err := encoder.MakeChannelCreationTransaction(newChannelId, genesisconfig.SampleConsortiumName, signer, nil, orgNames...)
	if err != nil {
		panic(err)
	}
	return env
}
