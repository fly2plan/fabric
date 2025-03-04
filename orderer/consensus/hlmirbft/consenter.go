/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package hlmirbft

import (
	"bytes"
	"path"
	"reflect"

	"code.cloudfoundry.org/clock"
	"github.com/fly2plan/fabric-protos-go/orderer/hlmirbft"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/types"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/orderer/consensus/inactive"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

//go:generate counterfeiter -o mocks/inactive_chain_registry.go --fake-name InactiveChainRegistry . InactiveChainRegistry

// InactiveChainRegistry registers chains that are inactive
type InactiveChainRegistry interface {
	// TrackChain tracks a chain with the given name, and calls the given callback
	// when this chain should be created.
	TrackChain(chainName string, genesisBlock *common.Block, createChain func())
	// Stop stops the InactiveChainRegistry. This is used when removing the
	// system channel.
	Stop()
}

//go:generate counterfeiter -o mocks/chain_manager.go --fake-name ChainManager . ChainManager

// ChainManager defines the methods from multichannel.Registrar needed by the Consenter.
type ChainManager interface {
	GetConsensusChain(channelID string) consensus.Chain
	CreateChain(channelID string)
	SwitchChainToFollower(channelID string)
	ReportConsensusRelationAndStatusMetrics(channelID string, relation types.ConsensusRelation, status types.Status)
}

// Config contains hlmirbft configurations
type Config struct {
	WALDir      string // WAL data of <my-channel> is stored in WALDir/<my-channel>
	SnapDir     string // Snaps of <my-channel> are stored in SnapDir/<my-channel>
	ReqStoreDir string // Requests of <my-channel> are stored in ReqStoreDir/<my-channel>
}

// Consenter implements hlmirbft consenter
type Consenter struct {
	ChainManager          ChainManager
	InactiveChainRegistry InactiveChainRegistry
	Dialer                *cluster.PredicateDialer
	Communication         cluster.Communicator
	*Dispatcher
	Logger         *flogging.FabricLogger
	HLMirBFTConfig Config
	OrdererConfig  localconfig.TopLevel
	Cert           []byte
	Metrics        *Metrics
	BCCSP          bccsp.BCCSP
}

// TargetChannel extracts the channel from the given proto.Message.
// Returns an empty string on failure.
func (c *Consenter) TargetChannel(message proto.Message) string {
	switch req := message.(type) {
	case *orderer.ConsensusRequest:
		return req.Channel
	case *orderer.SubmitRequest:
		return req.Channel
	default:
		return ""
	}
}

// ReceiverByChain returns the MessageReceiver for the given channelID or nil
// if not found.
func (c *Consenter) ReceiverByChain(channelID string) MessageReceiver {
	chain := c.ChainManager.GetConsensusChain(channelID)
	if chain == nil {
		return nil
	}
	if etcdRaftChain, isEtcdRaftChain := chain.(*Chain); isEtcdRaftChain {
		return etcdRaftChain
	}
	c.Logger.Warningf("Chain %s is of type %v and not etcdraft.Chain", channelID, reflect.TypeOf(chain))
	return nil
}

func (c *Consenter) detectSelfID(consenters map[uint64]*hlmirbft.Consenter) (uint64, error) {
	thisNodeCertAsDER, err := pemToDER(c.Cert, 0, "server", c.Logger)
	if err != nil {
		return 0, err
	}

	var serverCertificates []string
	for nodeID, cst := range consenters {
		serverCertificates = append(serverCertificates, string(cst.ServerTlsCert))

		certAsDER, err := pemToDER(cst.ServerTlsCert, nodeID, "server", c.Logger)
		if err != nil {
			return 0, err
		}

		if crypto.CertificatesWithSamePublicKey(thisNodeCertAsDER, certAsDER) == nil {
			return nodeID, nil
		}
	}

	c.Logger.Warning("Could not find", string(c.Cert), "among", serverCertificates)
	return 0, cluster.ErrNotInChannel
}

// HandleChain returns a new Chain instance or an error upon failure
func (c *Consenter) HandleChain(support consensus.ConsenterSupport, metadata *common.Metadata) (consensus.Chain, error) {
	m := &hlmirbft.ConfigMetadata{}
	if err := proto.Unmarshal(support.SharedConfig().ConsensusMetadata(), m); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal consensus metadata")
	}

	if m.Options == nil {
		return nil, errors.New("etcdraft options have not been provided")
	}

	isMigration := (metadata == nil || len(metadata.Value) == 0) && (support.Height() > 1)
	if isMigration {
		c.Logger.Debugf("Block metadata is nil at block height=%d, it is consensus-type migration", support.Height())
	}

	// determine raft replica set mapping for each node to its id
	// for newly started chain we need to read and initialize raft
	// metadata by creating mapping between consenter and its id.
	// In case chain has been restarted we restore raft metadata
	// information from the recently committed block meta data
	// field.
	blockMetadata, err := ReadBlockMetadata(metadata, m)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read MirBFT metadata")
	}

	consenters := CreateConsentersMap(blockMetadata, m)

	id, err := c.detectSelfID(consenters)
	if err != nil {
		if c.InactiveChainRegistry != nil {
			// There is a system channel, use the InactiveChainRegistry to track the
			// future config updates of application channel.
			c.InactiveChainRegistry.TrackChain(support.ChannelID(), support.Block(0), func() {
				c.ChainManager.CreateChain(support.ChannelID())
			})
			c.ChainManager.ReportConsensusRelationAndStatusMetrics(support.ChannelID(), types.ConsensusRelationConfigTracker, types.StatusInactive)
			return &inactive.Chain{Err: errors.Errorf("channel %s is not serviced by me", support.ChannelID())}, nil
		}

		return nil, errors.Wrap(err, "without a system channel, a follower should have been created")
	}

	opts := Options{
		MirBFTID: id,
		Clock:    clock.NewClock(),
		Logger:   c.Logger,

		HeartbeatTicks:       m.Options.HeartbeatTicks,
		SuspectTicks:         m.Options.SuspectTicks,
		NewEpochTimeoutTicks: m.Options.NewEpochTimeoutTicks,
		BufferSize:           m.Options.BufferSize,
		NumberOfBuckets:      m.Options.NumberOfBuckets,
		CheckpointInterval:   m.Options.CheckpointInterval,
		MaxEpochLength:       m.Options.MaxEpochLength,
		NodeStatuses:         m.Options.NodeStatuses,
		// TODO(harry_knight) is this needed?
		MaxSizePerMsg: uint64(support.SharedConfig().BatchSize().PreferredMaxBytes),

		BlockMetadata: blockMetadata,
		Consenters:    consenters,

		MigrationInit: isMigration,

		WALDir:      path.Join(c.HLMirBFTConfig.WALDir, support.ChannelID()),
		SnapDir:     path.Join(c.HLMirBFTConfig.SnapDir, support.ChannelID()),
		ReqStoreDir: path.Join(c.HLMirBFTConfig.ReqStoreDir, support.ChannelID()),
		Cert:        c.Cert,
		Metrics:     c.Metrics,
	}

	rpc := &cluster.RPC{
		Timeout:       c.OrdererConfig.General.Cluster.RPCTimeout,
		Logger:        c.Logger,
		Channel:       support.ChannelID(),
		Comm:          c.Communication,
		StreamsByType: cluster.NewStreamsByType(),
	}

	var haltCallback func() // called after the etcdraft.Chain halts when it detects eviction form the cluster.
	if c.InactiveChainRegistry != nil {
		// when we have a system channel, we use the InactiveChainRegistry to track membership upon eviction.
		c.Logger.Info("With system channel: after eviction InactiveChainRegistry.TrackChain will be called")
		haltCallback = func() {
			c.InactiveChainRegistry.TrackChain(support.ChannelID(), nil, func() { c.ChainManager.CreateChain(support.ChannelID()) })
			c.ChainManager.ReportConsensusRelationAndStatusMetrics(support.ChannelID(), types.ConsensusRelationConfigTracker, types.StatusInactive)
		}
	} else {
		// when we do NOT have a system channel, we switch to a follower.Chain upon eviction.
		c.Logger.Info("Without system channel: after eviction Registrar.SwitchToFollower will be called")
		haltCallback = func() { c.ChainManager.SwitchChainToFollower(support.ChannelID()) }
	}

	return NewChain(
		support,
		opts,
		c.Communication,
		rpc,
		c.BCCSP,
		func() (BlockPuller, error) {
			return NewBlockPuller(support, c.Dialer, c.OrdererConfig.General.Cluster, c.BCCSP)
		},
		haltCallback,
		nil,
	)
}

func (c *Consenter) IsChannelMember(joinBlock *common.Block) (bool, error) {
	if joinBlock == nil {
		return false, errors.New("nil block")
	}
	envelopeConfig, err := protoutil.ExtractEnvelope(joinBlock, 0)
	if err != nil {
		return false, err
	}
	bundle, err := channelconfig.NewBundleFromEnvelope(envelopeConfig, c.BCCSP)
	if err != nil {
		return false, err
	}
	oc, exists := bundle.OrdererConfig()
	if !exists {
		return false, errors.New("no orderer config in bundle")
	}
	configMetadata := &hlmirbft.ConfigMetadata{}
	if err := proto.Unmarshal(oc.ConsensusMetadata(), configMetadata); err != nil {
		return false, err
	}

	verifyOpts, err := createX509VerifyOptions(oc)
	if err != nil {
		return false, errors.Wrapf(err, "failed to create x509 verify options from orderer config")
	}

	if err := VerifyConfigMetadata(configMetadata, verifyOpts); err != nil {
		return false, errors.Wrapf(err, "failed to validate config metadata of ordering config")
	}

	member := false
	for _, consenter := range configMetadata.Consenters {
		if bytes.Equal(c.Cert, consenter.ServerTlsCert) || bytes.Equal(c.Cert, consenter.ClientTlsCert) {
			member = true
			break
		}
	}

	return member, nil
}

// RemoveInactiveChainRegistry stops and removes the inactive chain registry.
// This is used when removing the system channel.
func (c *Consenter) RemoveInactiveChainRegistry() {
	if c.InactiveChainRegistry == nil {
		return
	}
	c.InactiveChainRegistry.Stop()
	c.InactiveChainRegistry = nil
}

// ReadBlockMetadata attempts to read hlmirbft metadata from block metadata, if available.
// otherwise, it reads hlmirbft metadata from config metadata supplied.
func ReadBlockMetadata(blockMetadata *common.Metadata, configMetadata *hlmirbft.ConfigMetadata) (*hlmirbft.BlockMetadata, error) {
	if blockMetadata != nil && len(blockMetadata.Value) != 0 { // we have consenters mapping from block
		m := &hlmirbft.BlockMetadata{}
		if err := proto.Unmarshal(blockMetadata.Value, m); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal block's metadata")
		}
		return m, nil
	}

	m := &hlmirbft.BlockMetadata{
		NextConsenterId: 1,
		ConsenterIds:    make([]uint64, len(configMetadata.Consenters)),
	}
	// need to read consenters from the configuration
	for i := range m.ConsenterIds {
		m.ConsenterIds[i] = m.NextConsenterId
		m.NextConsenterId++
	}

	return m, nil
}

// New creates a hlmirbft Consenter
func New(
	clusterDialer *cluster.PredicateDialer,
	conf *localconfig.TopLevel,
	srvConf comm.ServerConfig,
	srv *comm.GRPCServer,
	registrar ChainManager,
	icr InactiveChainRegistry,
	metricsProvider metrics.Provider,
	bccsp bccsp.BCCSP,
) *Consenter {
	logger := flogging.MustGetLogger("orderer.consensus.hlmirbft")

	var cfg Config
	err := mapstructure.Decode(conf.Consensus, &cfg)
	if err != nil {
		logger.Panicf("Failed to decode hlmirbft configuration: %s", err)
	}

	consenter := &Consenter{
		ChainManager:          registrar,
		Cert:                  srvConf.SecOpts.Certificate,
		Logger:                logger,
		HLMirBFTConfig:        cfg,
		OrdererConfig:         *conf,
		Dialer:                clusterDialer,
		Metrics:               NewMetrics(metricsProvider),
		InactiveChainRegistry: icr,
		BCCSP:                 bccsp,
	}
	consenter.Dispatcher = &Dispatcher{
		Logger:        logger,
		ChainSelector: consenter,
	}

	comm := createComm(clusterDialer, consenter, conf.General.Cluster, metricsProvider)
	consenter.Communication = comm
	svc := &cluster.Service{
		CertExpWarningThreshold:          conf.General.Cluster.CertExpirationWarningThreshold,
		MinimumExpirationWarningInterval: cluster.MinimumExpirationWarningInterval,
		StreamCountReporter: &cluster.StreamCountReporter{
			Metrics: comm.Metrics,
		},
		StepLogger: flogging.MustGetLogger("orderer.common.cluster.step"),
		Logger:     flogging.MustGetLogger("orderer.common.cluster"),
		Dispatcher: comm,
	}
	orderer.RegisterClusterServer(srv.Server(), svc)

	if icr == nil {
		logger.Debug("Created an hlmirbft consenter without a system channel, InactiveChainRegistry is nil")
	}

	return consenter
}

func createComm(clusterDialer *cluster.PredicateDialer, c *Consenter, config localconfig.Cluster, p metrics.Provider) *cluster.Comm {
	metrics := cluster.NewMetrics(p)
	logger := flogging.MustGetLogger("orderer.common.cluster")

	compareCert := cluster.CachePublicKeyComparisons(func(a, b []byte) bool {
		err := crypto.CertificatesWithSamePublicKey(a, b)
		if err != nil && err != crypto.ErrPubKeyMismatch {
			crypto.LogNonPubKeyMismatchErr(logger.Errorf, err, a, b)
		}
		return err == nil
	})

	comm := &cluster.Comm{
		MinimumExpirationWarningInterval: cluster.MinimumExpirationWarningInterval,
		CertExpWarningThreshold:          config.CertExpirationWarningThreshold,
		SendBufferSize:                   config.SendBufferSize,
		Logger:                           logger,
		Chan2Members:                     make(map[string]cluster.MemberMapping),
		Connections:                      cluster.NewConnectionStore(clusterDialer, metrics.EgressTLSConnectionCount),
		Metrics:                          metrics,
		ChanExt:                          c,
		H:                                c,
		CompareCertificate:               compareCert,
	}
	c.Communication = comm
	return comm
}
