package channels

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	"github.com/lncapital/torq/pkg/server_errors"
	"github.com/rs/zerolog/log"
)

type failedUpdate struct {
	OutPoint struct {
		Txid    string
		OutIndx uint32
	}
	Reason      string
	UpdateError string
}

type updateResponse struct {
	Status        string         `json:"status"`
	FailedUpdates []failedUpdate `json:"failedUpdates"`
}

type updateChanRequestBody struct {
	ChannelPoint  *string `json:"channelPoint"`
	FeeRatePpm    *uint32 `json:"feeRatePpm"`
	BaseFeeMsat   *int64  `json:"baseFeeMsat"`
	MaxHtlcMsat   *uint64 `json:"maxHtlcMsat"`
	MinHtlcMsat   *uint64 `json:"minHtlcMsat"`
	TimeLockDelta uint32  `json:"timeLockDelta"`
}
type pendingChannel struct {
	PendingChannelPoint string `json:"pendingChannelPoint"`
}

func updateChannelsHandler(c *gin.Context, db *sqlx.DB) {
	requestBody := updateChanRequestBody{}

	if err := c.BindJSON(&requestBody); err != nil {
		log.Error().Msgf("JSON binding the request body")
		server_errors.WrapLogAndSendServerError(c, err, "JSON binding the request body")
		return
	}
	//log.Debug().Msgf("Received request body: %v", requestBody)

	response, err := updateChannels(db, requestBody)
	if err != nil {
		server_errors.WrapLogAndSendServerError(c, err, "Update channel/s policy")
		return
	}

	c.JSON(http.StatusOK, response)
}

type batchOpenChannel struct {
	NodePubkey         string `json:"nodePubkey"`
	LocalFundingAmount int64  `json:"localFundingAmount"`
	PushSat            *int64 `json:"pushSat"`
	Private            *bool  `json:"private"`
	MinHtlcMsat        *int64 `json:"minHtlcMsat"`
}

type BatchOpenRequest struct {
	Channels    []batchOpenChannel `json:"channels"`
	TargetConf  *int32             `json:"targetConf"`
	SatPerVbyte *int64             `json:"satPerVbyte"`
}

type BatchOpenResponse struct {
	PendingChannels []pendingChannel `json:"pendingChannels"`
}

func batchOpenHandler(c *gin.Context, db *sqlx.DB) {
	var batchOpnReq BatchOpenRequest
	if err := c.BindJSON(&batchOpnReq); err != nil {
		log.Error().Msgf("JSON binding the request body")
		server_errors.WrapLogAndSendServerError(c, err, "JSON binding the request body")
		return
	}

	response, err := batchOpenChannels(db, batchOpnReq)
	if err != nil {
		server_errors.WrapLogAndSendServerError(c, err, "Batch open channels")
		return
	}

	c.JSON(http.StatusOK, response)
}

func getStaticChannelBackupHandler(c *gin.Context, db *sqlx.DB) {
	response, err := getStaticChannelBackup(db)

	if err != nil {
		server_errors.WrapLogAndSendServerError(c, err, "Export all channel backups")
		return
	}

	c.JSON(http.StatusOK, response)
}

func RegisterChannelRoutes(r *gin.RouterGroup, db *sqlx.DB) {
	r.POST("update", func(c *gin.Context) { updateChannelsHandler(c, db) })
	r.POST("openbatch", func(c *gin.Context) { batchOpenHandler(c, db) })
	r.GET("static-channel-backup", func(c *gin.Context) { getStaticChannelBackupHandler(c, db) })
}
