/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package hlmirbft

import (
	"crypto/x509"
	"encoding/pem"
	"go.etcd.io/etcd/pkg/fileutil"
	"os"
	"path/filepath"
	"reflect"

	"github.com/fly2plan/fabric-protos-go/orderer/hlmirbft"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric-protos-go/orderer/etcdraft"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

type ConsentersMap map[string]struct{}

func (c ConsentersMap) Exists(consenter *hlmirbft.Consenter) bool {
	_, exists := c[string(consenter.ClientTlsCert)]
	return exists
}

// ConsentersToMap maps consenters into set where key is client TLS certificate
func ConsentersToMap(consenters []*hlmirbft.Consenter) ConsentersMap {
	set := map[string]struct{}{}
	for _, c := range consenters {
		set[string(c.ClientTlsCert)] = struct{}{}
	}
	return set
}

// MetadataHasDuplication returns an error if the metadata has duplication of consenters.
// A duplication is defined by having a server or a client TLS certificate that is found
// in two different consenters, regardless of the type of certificate (client/server).
func MetadataHasDuplication(md *hlmirbft.ConfigMetadata) error {
	if md == nil {
		return errors.New("nil metadata")
	}

	for _, consenter := range md.Consenters {
		if consenter == nil {
			return errors.New("nil consenter in metadata")
		}
	}

	seen := make(map[string]struct{})
	for _, consenter := range md.Consenters {
		serverKey := string(consenter.ServerTlsCert)
		clientKey := string(consenter.ClientTlsCert)
		_, duplicateServerCert := seen[serverKey]
		_, duplicateClientCert := seen[clientKey]
		if duplicateServerCert || duplicateClientCert {
			return errors.Errorf("duplicate consenter: server cert: %s, client cert: %s", serverKey, clientKey)
		}

		seen[serverKey] = struct{}{}
		seen[clientKey] = struct{}{}
	}
	return nil
}

// MetadataFromConfigValue reads and translates configuration updates from config value into raft metadata
func MetadataFromConfigValue(configValue *common.ConfigValue) (*hlmirbft.ConfigMetadata, error) {
	consensusTypeValue := &orderer.ConsensusType{}
	if err := proto.Unmarshal(configValue.Value, consensusTypeValue); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal consensusType config update")
	}

	updatedMetadata := &hlmirbft.ConfigMetadata{}
	if err := proto.Unmarshal(consensusTypeValue.Metadata, updatedMetadata); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal updated (new) etcdraft metadata configuration")
	}

	return updatedMetadata, nil
}

// MetadataFromConfigUpdate extracts consensus metadata from config update
func MetadataFromConfigUpdate(update *common.ConfigUpdate) (*hlmirbft.ConfigMetadata, error) {
	var baseVersion uint64
	if update.ReadSet != nil && update.ReadSet.Groups != nil {
		if ordererConfigGroup, ok := update.ReadSet.Groups["Orderer"]; ok {
			if val, ok := ordererConfigGroup.Values["ConsensusType"]; ok {
				baseVersion = val.Version
			}
		}
	}

	if update.WriteSet != nil && update.WriteSet.Groups != nil {
		if ordererConfigGroup, ok := update.WriteSet.Groups["Orderer"]; ok {
			if val, ok := ordererConfigGroup.Values["ConsensusType"]; ok {
				if baseVersion == val.Version {
					// Only if the version in the write set differs from the read-set
					// should we consider this to be an update to the consensus type
					return nil, nil
				}
				return MetadataFromConfigValue(val)
			}
		}
	}
	return nil, nil
}

// ConfigChannelHeader expects a config block and returns the header type
// of the config envelope wrapped in it, e.g. HeaderType_ORDERER_TRANSACTION
func ConfigChannelHeader(block *common.Block) (hdr *common.ChannelHeader, err error) {
	envelope, err := protoutil.ExtractEnvelope(block, 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract envelope from the block")
	}

	channelHeader, err := protoutil.ChannelHeader(envelope)
	if err != nil {
		return nil, errors.Wrap(err, "cannot extract channel header")
	}

	return channelHeader, nil
}

// ConfigEnvelopeFromBlock extracts configuration envelope from the block based on the
// config type, i.e. HeaderType_ORDERER_TRANSACTION or HeaderType_CONFIG
func ConfigEnvelopeFromBlock(block *common.Block) (*common.Envelope, error) {
	if block == nil {
		return nil, errors.New("nil block")
	}

	envelope, err := protoutil.ExtractEnvelope(block, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to extract envelope from the block")
	}

	channelHeader, err := protoutil.ChannelHeader(envelope)
	if err != nil {
		return nil, errors.Wrap(err, "cannot extract channel header")
	}

	switch channelHeader.Type {
	case int32(common.HeaderType_ORDERER_TRANSACTION):
		payload, err := protoutil.UnmarshalPayload(envelope.Payload)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal envelope to extract config payload for orderer transaction")
		}
		configEnvelop, err := protoutil.UnmarshalEnvelope(payload.Data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal config envelope for orderer type transaction")
		}

		return configEnvelop, nil
	case int32(common.HeaderType_CONFIG):
		return envelope, nil
	default:
		return nil, errors.Errorf("unexpected header type: %v", channelHeader.Type)
	}
}

// ConsensusMetadataFromConfigBlock reads consensus metadata updates from the configuration block
func ConsensusMetadataFromConfigBlock(block *common.Block) (*hlmirbft.ConfigMetadata, error) {
	if block == nil {
		return nil, errors.New("nil block")
	}

	if !protoutil.IsConfigBlock(block) {
		return nil, errors.New("not a config block")
	}

	configEnvelope, err := ConfigEnvelopeFromBlock(block)
	if err != nil {
		return nil, errors.Wrap(err, "cannot read config update")
	}

	payload, err := protoutil.UnmarshalPayload(configEnvelope.Payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract payload from config envelope")
	}
	// get config update
	configUpdate, err := configtx.UnmarshalConfigUpdateFromPayload(payload)
	if err != nil {
		return nil, errors.Wrap(err, "could not read config update")
	}

	return MetadataFromConfigUpdate(configUpdate)
}

// VerifyConfigMetadata validates Raft config metadata.
// Note: ignores certificates expiration.
func VerifyConfigMetadata(metadata *hlmirbft.ConfigMetadata, verifyOpts x509.VerifyOptions) error {
	if metadata == nil {
		// defensive check. this should not happen as CheckConfigMetadata
		// should always be called with non-nil config metadata
		return errors.Errorf("nil Raft config metadata")
	}

	if metadata.Options == nil {
		return errors.Errorf("nil Raft config metadata options")
	}

	if metadata.Options.HeartbeatTicks == 0 ||
		metadata.Options.NewEpochTimeoutTicks == 0 ||
		metadata.Options.SuspectTicks == 0 {
		return errors.Errorf("none of HeartbeatTicks (%d), NewEpochTimeoutTicks (%d) and SuspectTicks (%d) can be zero",
			metadata.Options.HeartbeatTicks, metadata.Options.NewEpochTimeoutTicks, metadata.Options.SuspectTicks)
	}

	// check Mir-BFT options
	if metadata.Options.NewEpochTimeoutTicks <= metadata.Options.HeartbeatTicks {
		return errors.Errorf("NewEpochTimeoutTicks (%d) must be greater than HeartbeatTicks (%d)",
			metadata.Options.NewEpochTimeoutTicks, metadata.Options.HeartbeatTicks)
	}

	if metadata.Options.SuspectTicks <= metadata.Options.HeartbeatTicks {
		return errors.Errorf("SuspectTicks (%d) must be greater than HeartbeatTicks (%d)",
			metadata.Options.SuspectTicks, metadata.Options.HeartbeatTicks)
	}

	if len(metadata.Consenters) == 0 {
		return errors.Errorf("empty consenter set")
	}

	// verifying certificates for being signed by CA, expiration is ignored
	for _, consenter := range metadata.Consenters {
		if consenter == nil {
			return errors.Errorf("metadata has nil consenter")
		}
		if err := validateConsenterTLSCerts(consenter, verifyOpts, true); err != nil {
			return err
		}
	}

	if err := MetadataHasDuplication(metadata); err != nil {
		return err
	}

	return nil
}

func parseCertificateFromBytes(cert []byte) (*x509.Certificate, error) {
	pemBlock, _ := pem.Decode(cert)
	if pemBlock == nil {
		return &x509.Certificate{}, errors.Errorf("no PEM data found in cert[% x]", cert)
	}

	certificate, err := x509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		return nil, errors.Errorf("%s TLS certificate has invalid ASN1 structure %s", err, string(pemBlock.Bytes))
	}

	return certificate, nil
}

func parseCertificateListFromBytes(certs [][]byte) ([]*x509.Certificate, error) {
	var certificateList []*x509.Certificate

	for _, cert := range certs {
		certificate, err := parseCertificateFromBytes(cert)
		if err != nil {
			return certificateList, err
		}

		certificateList = append(certificateList, certificate)
	}

	return certificateList, nil
}

func createX509VerifyOptions(ordererConfig channelconfig.Orderer) (x509.VerifyOptions, error) {
	tlsRoots := x509.NewCertPool()
	tlsIntermediates := x509.NewCertPool()

	for _, org := range ordererConfig.Organizations() {
		rootCerts, err := parseCertificateListFromBytes(org.MSP().GetTLSRootCerts())
		if err != nil {
			return x509.VerifyOptions{}, errors.Wrap(err, "parsing tls root certs")
		}
		intermediateCerts, err := parseCertificateListFromBytes(org.MSP().GetTLSIntermediateCerts())
		if err != nil {
			return x509.VerifyOptions{}, errors.Wrap(err, "parsing tls intermediate certs")
		}

		for _, cert := range rootCerts {
			tlsRoots.AddCert(cert)
		}

		for _, cert := range intermediateCerts {
			tlsIntermediates.AddCert(cert)
		}
	}

	return x509.VerifyOptions{
		Roots:         tlsRoots,
		Intermediates: tlsIntermediates,
		KeyUsages: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
	}, nil
}

// validateConsenterTLSCerts decodes PEM cert, parses and validates it.
func validateConsenterTLSCerts(c *hlmirbft.Consenter, opts x509.VerifyOptions, ignoreExpiration bool) error {
	clientCert, err := parseCertificateFromBytes(c.ClientTlsCert)
	if err != nil {
		return errors.Wrapf(err, "parsing tls client cert of %s:%d", c.Host, c.Port)
	}

	serverCert, err := parseCertificateFromBytes(c.ServerTlsCert)
	if err != nil {
		return errors.Wrapf(err, "parsing tls server cert of %s:%d", c.Host, c.Port)
	}

	verify := func(certType string, cert *x509.Certificate, opts x509.VerifyOptions) error {
		if _, err := cert.Verify(opts); err != nil {
			if validationRes, ok := err.(x509.CertificateInvalidError); !ok || (!ignoreExpiration || validationRes.Reason != x509.Expired) {
				return errors.Wrapf(err, "verifying tls %s cert with serial number %d", certType, cert.SerialNumber)
			}
		}
		return nil
	}

	if err := verify("client", clientCert, opts); err != nil {
		return err
	}
	if err := verify("server", serverCert, opts); err != nil {
		return err
	}

	return nil
}

// ConsenterCertificate denotes a TLS certificate of a consenter
type ConsenterCertificate struct {
	ConsenterCertificate []byte
	CryptoProvider       bccsp.BCCSP
	Logger               *flogging.FabricLogger
}

// IsConsenterOfChannel returns whether the caller is a consenter of a channel
// by inspecting the given configuration block.
// It returns nil if true, else returns an error.
func (conCert ConsenterCertificate) IsConsenterOfChannel(configBlock *common.Block) error {
	if configBlock == nil || configBlock.Header == nil {
		return errors.New("nil block or nil header")
	}
	envelopeConfig, err := protoutil.ExtractEnvelope(configBlock, 0)
	if err != nil {
		return err
	}
	bundle, err := channelconfig.NewBundleFromEnvelope(envelopeConfig, conCert.CryptoProvider)
	if err != nil {
		return err
	}
	oc, exists := bundle.OrdererConfig()
	if !exists {
		return errors.New("no orderer config in bundle")
	}
	m := &etcdraft.ConfigMetadata{}
	if err := proto.Unmarshal(oc.ConsensusMetadata(), m); err != nil {
		return err
	}

	bl, _ := pem.Decode(conCert.ConsenterCertificate)
	if bl == nil {
		return errors.Errorf("my consenter certificate %s is not a valid PEM", string(conCert.ConsenterCertificate))
	}

	myCertDER := bl.Bytes

	var failedMatches []string
	for _, consenter := range m.Consenters {
		candidateBlock, _ := pem.Decode(consenter.ServerTlsCert)
		if candidateBlock == nil {
			return errors.Errorf("candidate server certificate %s is not a valid PEM", string(consenter.ServerTlsCert))
		}
		sameServerCertErr := crypto.CertificatesWithSamePublicKey(myCertDER, candidateBlock.Bytes)

		candidateBlock, _ = pem.Decode(consenter.ClientTlsCert)
		if candidateBlock == nil {
			return errors.Errorf("candidate client certificate %s is not a valid PEM", string(consenter.ClientTlsCert))
		}
		sameClientCertErr := crypto.CertificatesWithSamePublicKey(myCertDER, candidateBlock.Bytes)

		if sameServerCertErr == nil || sameClientCertErr == nil {
			return nil
		}
		conCert.Logger.Debugf("I am not %s:%d because %s, %s", consenter.Host, consenter.Port, sameServerCertErr, sameClientCertErr)
		failedMatches = append(failedMatches, string(consenter.ClientTlsCert))
	}
	conCert.Logger.Debugf("Failed matching our certificate %s against certificates encoded in config block %d: %v",
		string(conCert.ConsenterCertificate),
		configBlock.Header.Number,
		failedMatches)

	return cluster.ErrNotInChannel
}

// NodeExists returns trues if node id exists in the slice
// and false otherwise
func NodeExists(id uint64, nodes []uint64) bool {
	for _, nodeID := range nodes {
		if nodeID == id {
			return true
		}
	}
	return false
}

// CreateConsentersMap creates a map of Raft Node IDs to Consenter given the block metadata and the config metadata.
func CreateConsentersMap(blockMetadata *hlmirbft.BlockMetadata, configMetadata *hlmirbft.ConfigMetadata) map[uint64]*hlmirbft.Consenter {
	consenters := map[uint64]*hlmirbft.Consenter{}
	for i, consenter := range configMetadata.Consenters {
		consenters[blockMetadata.ConsenterIds[i]] = consenter
	}
	return consenters
}

//JIRA FLY2-66 : Remove Files
func PurgeFiles(files []string, dirPath string, logger *flogging.FabricLogger) error {

	for _, file := range files {

		fpath := filepath.Join(dirPath, file)

		l, err := fileutil.TryLockFile(fpath, os.O_WRONLY, fileutil.PrivateFileMode)

		if err != nil {

			logger.Debugf("Failed to lock %s, abort purging", file)

			break
		}

		if err = os.Remove(fpath); err != nil {

			logger.Errorf("Failed to remove %s: %s", file, err)

		} else {

			logger.Debugf("Purged file %s", file)

		}

		if err = l.Close(); err != nil {

			logger.Errorf("Failed to close file lock %s: %s", l.Name(), err)
		}
	}

	return nil

}

//JIRA FLY2-103 : Function to identify the removed consenter details by comparing the current and updated consenter list
func CompareConsenterList(existingConsenters, updatedConsenters []*hlmirbft.Consenter) *hlmirbft.Consenter {
	consenter_Map := make(map[*hlmirbft.Consenter]bool, len(existingConsenters))
	for _, value := range existingConsenters {
		consenter_Map[value] = true
	}
	for _, value := range updatedConsenters {
		if !consenter_Map[value] {
			return value
		}
	}
	return nil
}

//JIRA FLY2-103 : Function to fetch the consenter ID using the consenter details
func GetConsenterId(consenters map[uint64]*hlmirbft.Consenter, consenter *hlmirbft.Consenter) (key uint64, ok bool) {
	for k, v := range consenters {
		if reflect.DeepEqual(v, consenter) {
			key = k
			ok = true
			return key, ok
		}
	}
	return
}

//JIRA FLY2-103 : Function to remove nodeID from NodeID list
func removeNodeID(nodeList []uint64, nodeID uint64) []uint64 {
	var index int
	for i := 0; i < len(nodeList); i++ {
		if nodeList[i] == nodeID {
			index = i
		}
	}
	newNodeList := append(nodeList[:index], nodeList[index+1:]...)
	return newNodeList
}

//JIRA FLY2-103 - pop the first element from pending reconfigurations
func PopReconfiguration(configEnvs []pendingConfigEnvelope) (newPendingReconfig []pendingConfigEnvelope) {
	if len(configEnvs) > 1 {
		configEnvs = configEnvs[1:]
	} else {
		configEnvs = nil
	}
	return configEnvs
}
