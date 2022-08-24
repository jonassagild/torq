package channels

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/cockroachdb/errors"
	"github.com/jmoiron/sqlx"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lncapital/torq/internal/settings"
	"github.com/lncapital/torq/pkg/lnd_connect"
	"github.com/rs/zerolog/log"
	"io"
)

type OpenChannelRequest struct {
	NodePubKey         string  `json:"nodePubKey"`
	LocalFundingAmount int64   `json:"localFundingAmount"`
	PushSat            *int64  `json:"pushSat"`
	SatPerVbyte        *uint64 `json:"satPerVbyte"`
	TargetConf         *int32  `json:"targetConf"`
	Private            *bool   `json:"private"`
	MinHtlcMsat        *int64  `json:"minHtlcMsat"`
	RemoteCsvDelay     *uint32 `json:"remoteCsvDelay"`
	MinConfs           *int32  `json:"minConfs"`
	SpendUnconfirmed   *bool   `json:"spendUnconfirmed"`
	CloseAddress       *string `json:"closeAddress"`
}

type OpenChannelResponse struct {
	reqId               string `json:"reqId"`
	Status              string `json:"status"`
	ChannelPoint        string `json:"channelPoint,omitempty"`
	PendingChannelPoint string `json:"pendingChannelPoint,omitempty"`
}

type PsbtDetails struct {
	FundingAddress string `json:"funding_address,omitempty"`
	FundingAmount  int64  `json:"funding_amount,omitempty"`
	Psbt           []byte `json:"psbt,omitempty"`
}

func OpenChannel(db *sqlx.DB, wChan chan interface{}, req OpenChannelRequest, reqId string) (err error) {

	if req.SatPerVbyte != nil && req.TargetConf != nil {
		return errors.New("Cannot set both SatPerVbyte and TargetConf")
	}

	pubKeyHex, err := hex.DecodeString(req.NodePubKey)
	if err != nil {
		return errors.New("error decoding public key hex")
	}

	//open channel request
	openChanReq := lnrpc.OpenChannelRequest{
		NodePubkey: pubKeyHex,

		// This is the amount we are putting into the channel (channel size)
		LocalFundingAmount: req.LocalFundingAmount,
	}

	// The amount to give the other node in the opening process.
	// NB: This means you will give the other node this amount of sats
	if req.PushSat != nil {
		openChanReq.PushSat = *req.PushSat
	}

	if req.SatPerVbyte != nil {
		openChanReq.SatPerVbyte = *req.SatPerVbyte
	}

	if req.TargetConf != nil {
		openChanReq.TargetConf = *req.TargetConf
	}

	if req.Private != nil {
		openChanReq.Private = *req.Private
	}

	if req.MinHtlcMsat != nil {
		openChanReq.MinHtlcMsat = *req.MinHtlcMsat
	}

	if req.RemoteCsvDelay != nil {
		openChanReq.RemoteCsvDelay = *req.RemoteCsvDelay
	}

	if req.MinConfs != nil {
		openChanReq.MinConfs = *req.MinConfs
	}

	if req.SpendUnconfirmed != nil {
		openChanReq.SpendUnconfirmed = *req.SpendUnconfirmed
	}

	if req.CloseAddress != nil {
		openChanReq.CloseAddress = *req.CloseAddress
	}

	connectionDetails, err := settings.GetConnectionDetails(db)
	if err != nil {
		log.Error().Err(err).Msgf("Error getting node connection details from the db: %s", err.Error())
		return errors.New("Error getting node connection details from the db")
	}

	conn, err := lnd_connect.Connect(
		connectionDetails.GRPCAddress,
		connectionDetails.TLSFileBytes,
		connectionDetails.MacaroonFileBytes)
	if err != nil {
		log.Error().Err(err).Msgf("can't connect to LND: %s", err.Error())
		return errors.Newf("can't connect to LND")
	}
	defer conn.Close()

	client := lnrpc.NewLightningClient(conn)

	ctx := context.Background()
	//Send open channel request
	openChanRes, err := client.OpenChannel(ctx, &openChanReq)
	if err != nil {
		log.Error().Msgf("Err opening channel: %v", err)
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		resp, err := openChanRes.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			log.Error().Msgf("could not open channel: %v", err)
			wChan <- errors.Newf("could not open channel: %v", err)
			return err
		}

		r, err := processOpenResponse(resp)
		if err != nil {
			return err
		}
		wChan <- r

	}
	return nil
}

func processOpenResponse(resp *lnrpc.OpenStatusUpdate) (*OpenChannelResponse, error) {

	switch resp.GetUpdate().(type) {
	case *lnrpc.OpenStatusUpdate_ChanPending:
		log.Info().Msgf("Channel pending")

		pc := resp.GetChanPending()
		pcp, err := translateChanPoint(pc.Txid, pc.OutputIndex)
		if err != nil {
			log.Error().Msgf("Error translating pending channel point")
			return nil, err
		}

		return &OpenChannelResponse{
			Status:              "PENDING",
			PendingChannelPoint: pcp,
		}, nil

	case *lnrpc.OpenStatusUpdate_ChanOpen:
		log.Info().Msgf("Channel open")

		oc := resp.GetChanOpen()
		ocp, err := translateChanPoint(oc.ChannelPoint.GetFundingTxidBytes(), oc.ChannelPoint.OutputIndex)
		if err != nil {
			log.Error().Msgf("Error translating channel point")
			return nil, err
		}

		return &OpenChannelResponse{
			Status:       "OPEN",
			ChannelPoint: ocp,
		}, nil

	case *lnrpc.OpenStatusUpdate_PsbtFund:
		log.Error().Msg("Channel psbt fund response received. Can't process this response")
		return nil, errors.New("Channel psbt fund response received. Can't process this response")
	default:
	}

	return nil, nil
}

func translateChanPoint(cb []byte, oi uint32) (string, error) {
	ch, err := chainhash.NewHash(cb)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", ch.String(), oi), nil
}
