package channels

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/jmoiron/sqlx"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lncapital/torq/internal/settings"
	"github.com/lncapital/torq/pkg/lnd_connect"
	"github.com/rs/zerolog/log"
)

func getStaticChannelBackup(db *sqlx.DB, requestBody lnrpc.ChanBackupExportRequest) (r lnrpc.ChannelBackups, err error) {

	connectionDetails, err := settings.GetConnectionDetails(db)
	conn, err := lnd_connect.Connect(
		connectionDetails[0].GRPCAddress,
		connectionDetails[0].TLSFileBytes,
		connectionDetails[0].MacaroonFileBytes)
	if err != nil {
		log.Error().Err(err).Msgf("can't connect to LND: %s", err.Error())
		return r, errors.Newf("can't connect to LND")
	}

	defer conn.Close()

	client := lnrpc.NewLightningClient(conn)
	ctx := context.Background()

	resp, err := client.ExportAllChannelBackups(ctx, &requestBody)
	if err != nil {
		log.Error().Msgf("Error exporting all channel backups: %v", err)
		return r, err
	}

	staticChannelBackupResp := *resp.SingleChanBackups

	return staticChannelBackupResp, err
}
