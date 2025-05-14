import { Op } from "sequelize";
import { config } from "../config/config";
import { TrnxBlockchainStatus } from "../enum";
import { kafkaHelper, trnx_history_queries } from "../helpers";

class ETHPendingWithdrawalProcess {
  public trx_pending_withdrawal_tx_topic = config.KAFKA_TRANSACTIONS.TRX_PENDING_WITHDRAWAL_TX_TOPIC;

  public getTransactionFromDB = async () => {
    let tx: any = await trnx_history_queries.trnx_history_find_all(
      ["tx_id", "coin_id"],
      {
        status: 'completed',
        [Op.or]: [{ blockchain_status: null }, { blockchain_status: TrnxBlockchainStatus.PENDING }],
        req_type: { [Op.notIn]: ['TRANSAK', 'ALCHEMY', 'MERCURYO'] },
        coin_family: config.STATIC_COIN_FAMILY.TRX,
        tx_id: { [Op.not]: null }
      },
    )

    if (tx.length > 0) {
      for await (const el of tx) {
        console.log("el>>>", el)
        await this.addTxToQueue(el);
      }
    }
  };

  public addTxToQueue = async (data: { tx_id: string; coin_id: number }) => {
    await kafkaHelper.produceMessage(this.trx_pending_withdrawal_tx_topic, data);
  };
}

export const ethPendingWithdrawalProcess = new ETHPendingWithdrawalProcess();
