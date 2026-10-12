import { Op } from "sequelize";
import { config } from "../config/config";
import { TrnxBlockchainStatus } from "../enum";
import { kafkaHelper } from "../helpers";
import { CoinsModel, TrnxHistoryModel } from "../models/model";
import { pending_trnx } from "../interface";

class ETHPendingWithdrawalProcess {
  public trx_pending_withdrawal_tx_topic = config.KAFKA_TRANSACTIONS.TRX_PENDING_WITHDRAWAL_TX_TOPIC;

  public getTransactionFromDB = async () => {
    const tx: any = await TrnxHistoryModel.findAll({
      attributes: ["tx_id", "coin_id"],
      include: [{
        model: CoinsModel,
        attributes: ['is_token', 'token_type', 'token_address'],
        as: "coin_data"
      }],
      where: {
        status: 'completed',
        [Op.or]: [{ blockchain_status: null }, { blockchain_status: TrnxBlockchainStatus.PENDING }],
        req_type: { [Op.in]: ['APP', 'EXNG'] },
        coin_family: config.STATIC_COIN_FAMILY.TRX
      },
    })
    if (tx.length > 0) {
      await Promise.all(tx.map(async (el: any) => {
        const trnx: pending_trnx = {
          tx_id: el.tx_id,
          coin_id: el.coin_id,
          is_token: el.coin_data.is_token,
          token_type: el.coin_data.token_type,
          token_address: el.coin_data.token_address,
        };
        return this.addTxToQueue(trnx); // Directly return the promise
      }));
    }
  };

  public addTxToQueue = async (data: { tx_id: string; coin_id: number }) => {
    await kafkaHelper.produceMessage(this.trx_pending_withdrawal_tx_topic, data);
  };
}

export const ethPendingWithdrawalProcess = new ETHPendingWithdrawalProcess();
