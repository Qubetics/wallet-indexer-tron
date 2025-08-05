import { config } from "../config/config";
import { BooleanEnum, GlblBoolean, TRONSTATUS, TokenStandard, TrnxBlockchainStatus, TrnxStatusEnum, TrnxTypeEnum } from "../enum";
import { Utility_Helper, Blockchain_Helper, node_issue_error_log_queries, trnx_history_queries, kafkaHelper } from "../helpers";
import { pending_trnx } from "../interface";
import tronProcessHelper from "./processes.helper";
import { Op } from "sequelize";
class TronTxStatusUpdateProcess {
  public tron_coin_family: any = config.STATIC_COIN_FAMILY.TRX;
  public trx_pending_withdrawal_tx_group_id = config.KAFKA_TRANSACTIONS.TRX_PENDING_WITHDRAWAL_TX_GROUP_ID;
  public trx_pending_withdrawal_tx_topic = config.KAFKA_TRANSACTIONS.TRX_PENDING_WITHDRAWAL_TX_TOPIC;

  public startTxStatusUpdateQueue = async () => {
    let consumer = await kafkaHelper.kafka.consumer({ groupId: this.trx_pending_withdrawal_tx_group_id });
    await consumer.subscribe({ topic: this.trx_pending_withdrawal_tx_topic, fromBeginning: true });
    await consumer.run({
      eachMessage: async ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
        console.log("🚀 ~ startTxStatusUpdateQueue topic >>>>", topic);
        console.log("🚀 ~ startTxStatusUpdateQueue message.value >>>>", message.value.toString());
        await this.getTx(JSON.parse(message.value.toString()))
      }
    }).catch((err: Error) => {
      console.log("🔥 ~ ~ startTxStatusUpdateQueue error", err);
    });
  };

  public async getTx(data: pending_trnx) {
    try {
      console.log('Pending data >>>>>>>', data);
      let trnx_fee: number = GlblBoolean.false;
      if (data) {
        const transaction: any = await Blockchain_Helper.GetConfirmedTransaction(data.tx_id, 0)
        if (transaction.status) {
          const TransactionInfo: any = await Blockchain_Helper.GetTransactionInfo(data.tx_id);
          const transactionData = TransactionInfo.data;
          trnx_fee = await Blockchain_Helper.tronWeb.fromSun(transactionData.fee);
          const blockNumber: number = transactionData.blockNumber;
          if (transaction.data.ret[GlblBoolean.false]) {
            const contractRet: string = transaction.data.ret[GlblBoolean.false].contractRet;
            let txData = {
              coin: {
                coin_id: data.coin_id,
                is_token: data.is_token,
                token_type: data.token_type,
                token_address: data.token_address,
              },
              txid: transactionData.id,
              blockchain_status: TrnxBlockchainStatus.PENDING || null,
              coin_family: config.STATIC_COIN_FAMILY.TRX,
              tx_fee: Number(trnx_fee),
              block_number: blockNumber,
              status: TrnxStatusEnum.COMPLETED
            };
            if (contractRet == TRONSTATUS.SUCCESS) {
              txData.status = TrnxStatusEnum.COMPLETED;
              txData.blockchain_status = TrnxBlockchainStatus.CONFIRMED;
            } else if (contractRet == TRONSTATUS.REVERT) {
              txData.blockchain_status = TrnxBlockchainStatus.FAILED;
              txData.status = TrnxStatusEnum.COMPLETED;
            } else if (contractRet == TRONSTATUS.OUT_OF_ENERGY) {
              txData.blockchain_status = TrnxBlockchainStatus.FAILED;
              txData.status = TrnxStatusEnum.COMPLETED;
            }
            await tronTxStatusUpdateProcess.sendTxUpdate(txData);
          } else {
            let txData = {
              coin: {
                coin_id: data.coin_id,
                is_token: data.is_token,
                token_type: data.token_type,
                token_address: data.token_address,
              },
              txid: transactionData.id,
              blockchain_status: TrnxBlockchainStatus.FAILED,
              coin_family: config.STATIC_COIN_FAMILY.TRX,
              tx_fee: Number(trnx_fee),
              block_number: blockNumber,
              status: TrnxStatusEnum.COMPLETED
            };
            await tronTxStatusUpdateProcess.sendTxUpdate(txData);
          }
        } else {
          console.log(`transaction status false >>>`, transaction);

          let count_data: any = await trnx_history_queries.trnx_history_find_one(
            ["queue_check_count", "blockchain_status"],
            {
              tx_id: data.tx_id,
              [Op.or]: [{ blockchain_status: TrnxBlockchainStatus.PENDING }]
            }
          )
          if (count_data.queue_check_count == 21) {
            if (count_data.blockchain_status == TrnxBlockchainStatus.PENDING) {
              await trnx_history_queries.trnx_history_update({ blockchain_status: TrnxBlockchainStatus.FAILED }, { tx_id: data.tx_id })
            } else {
              console.log("Else of count_data.blockchain_status == pending")
            }
          } else {
            await trnx_history_queries.trnx_history_update({ queue_check_count: count_data.queue_check_count + 1 }, { tx_id: data.tx_id })
          }
        }
      }
    } catch (err: any) {
      console.error("Error in getTx>>", err);
      await node_issue_error_log_queries.node_issue_error_logs_create({
        function: "getTx",
        block_number: "0",
        error: err.message,
        transaction_id: data.tx_id,
        from_adrs: null,
        to_adrs: null,
        coin_family: config.STATIC_COIN_FAMILY.TRX,
        extra: "catch under getTx"
      })
    }
  };

  public sendTxUpdate = async (txData: {
    coin: {
      coin_id: number,
      is_token: boolean,
      token_type: string | null,
      token_address: string
    };
    txid: string;
    blockchain_status: string,
    coin_family: number,
    tx_fee: number
    block_number: number,
    status: string;
  }) => {
    try {
      let coin_data: any = {};
      if (!txData.coin?.is_token) {
        console.log('in case of coin');
        coin_data = await tronProcessHelper.checkIfContract('NativeCoin');
      } else if (txData.coin?.token_type?.toLowerCase() == TokenStandard.TRC20) {
        coin_data = await tronProcessHelper.checkIfContract(txData.coin.token_address);
      }

      //let coin_data: any = await tronProcessHelper.CoinByCoinId(txData.coin_id);
      const trx_data = {
        coin_id: txData?.coin.coin_id,
        txid: txData?.txid,
        blockchain_status: txData?.blockchain_status,
        tx_fee: txData?.tx_fee,
        block_number: txData?.block_number,
        status: txData?.status,
        gas_reverted: null,
        fromAddress: '',
        coin: {
          coin_id: txData?.coin.coin_id,
          is_token: coin_data.is_token,
          coin_symbol: coin_data.coin_symbol,
          coin_family: coin_data.coin_family,
          token_type: coin_data.token_type,
          token_address: Number(coin_data.is_token) === Number(BooleanEnum.true) ? coin_data.token_address : coin_data.coin_symbol,
          added_by: coin_data.added_by
        }
      };
      await this.updateBroadcastTx(trx_data);
    } catch (err: any) {
      console.error('error in  sendTxUpdate error >>>>>>>', err);
      return console.error(err.message);
    }
  };

  public async updateBroadcastTx(trx_data: any) {
    console.log("Entered into updateBroadcastTx>>>")

    let txnData: any = await trnx_history_queries.trnx_history_find_one(
      ['from_adrs', 'amount', 'user_id', 'id', 'to_adrs', 'type', 'coin_family'],
      {
        tx_id: trx_data.txid, status: TrnxStatusEnum.COMPLETED,
        [Op.or]: [
          { blockchain_status: null },
          { blockchain_status: TrnxBlockchainStatus.PENDING }
        ]
      }
    )
    console.log("txnData>>", txnData)
    if (txnData) {
      const from_address = txnData.from_adrs;
      const amount = txnData.amount;
      const user_id = txnData.user_id;
      const tx_row_id = txnData.id;
      const to_address = txnData.to_adrs;
      const tx_type = txnData?.type
      const coin_symbol = trx_data?.coin?.coin_symbol
      const coin_family = txnData.coin_family;
      const blockchain_status: any = trx_data.blockchain_status
      console.log("txnData going to update>>", txnData.id, "blockchain_status", blockchain_status)

      const txUpdateRes: any = await trnx_history_queries.trnx_history_update(
        { blockchain_status: blockchain_status, block_id: trx_data.block_number, tx_fee: trx_data.tx_fee, gas_reverted: trx_data.gas_reverted },
        { id: tx_row_id }
      )

      if (txUpdateRes) {
        console.log("under txUpdateRes txnData going to update>>", txnData.id, "blockchain_status", blockchain_status)

        if (blockchain_status == TrnxBlockchainStatus.CONFIRMED) {

          const AddressWithdraw: any = await tronProcessHelper.check_our_wallet_address(from_address);

          if (AddressWithdraw) {
            console.log('AddressWithdraw >>>>>>>>>>>>>', tx_type, AddressWithdraw);
            if (tx_type !== TrnxTypeEnum.DAPP) {
              console.log('tx_type >>>>>>>>>>>>>', tx_type);
              const native_coin: any = await tronProcessHelper.checkIfContract('NativeCoin');
              console.log("789 adding_123coins_to_queusssse ", native_coin)

              await tronProcessHelper.addingCoinsToQueue(from_address, native_coin)

              if (trx_data?.coin?.is_token === 1 && trx_data.coin?.token_type.toLowerCase() === TokenStandard.TRC20.toLowerCase()) {
                console.log("123 adding_123coins_to_queue ", trx_data?.coin)

                await tronProcessHelper.addingCoinsToQueue(from_address, trx_data?.coin)

              }
            } else {
              await tronProcessHelper.addingAddressToQueue(from_address, coin_family)
            }

            let trnxTypeW: string = "Withdraw";
            switch (tx_type) {
              case TrnxTypeEnum.DAPP:
                trnxTypeW = "Smart Contract Execution";
                break;
              case TrnxTypeEnum.APPROVE:
                trnxTypeW = "Approval";
                break;
              case TrnxTypeEnum.SWAP:
                trnxTypeW = "Swap";
                break;
              case TrnxTypeEnum.CROSS_CHAIN:
                trnxTypeW = "Cross Chain Swap";
                break;
              default:
                break;
            }
            const notiMsg = `${trnxTypeW} of ${await Utility_Helper.bigNumberSafeConversion(
              amount
            )} ${coin_symbol.toUpperCase()} has been confirmed.`;

            const notifData: any = {
              title: trnxTypeW.toUpperCase(),
              message: notiMsg,
              from_user_id: 0,
              amount: amount,
              to_user_id: user_id,
              coin_symbol: coin_symbol,
              wallet_address: from_address,
              tx_id: tx_row_id,
              coin_id: trx_data?.coin?.coin_id,
              coin_family: trx_data?.coin.coin_family,
              tx_type: tx_type,
              notification_type: trnxTypeW,
              state: TrnxBlockchainStatus.CONFIRMED
            };

            Utility_Helper.SendNotification(notifData);
          }

          const Addressdeposit: any = await tronProcessHelper.check_our_wallet_address(to_address);
          if (Addressdeposit) {
            let to_user_id: number = Addressdeposit ? Addressdeposit.user_id : 0;
            let trnxTypeD: string = "Deposit";

            if (to_address !== from_address) {
              console.log("ENtering addingCoinsToQueue ")
              await tronProcessHelper.addingCoinsToQueue(to_address, trx_data.coin)
            }

            const notiMsg = `${trnxTypeD} of ${await Utility_Helper.bigNumberSafeConversion(
              amount
            )} ${trx_data?.coin?.coin_symbol.toUpperCase()} has been confirmed.`;

            const notifData: any = {
              title: trnxTypeD.toUpperCase(),
              message: notiMsg,
              amount: amount,
              from_user_id: user_id,
              to_user_id: to_user_id,
              wallet_address: to_address,
              tx_id: tx_row_id,
              coin_symbol: coin_symbol,
              coin_id: trx_data?.coin?.coin_id,
              coin_family: trx_data?.coin.coin_family,
              tx_type: tx_type,
              notification_type: trnxTypeD,
              state: TrnxBlockchainStatus.CONFIRMED
            };
            Utility_Helper.SendNotification(notifData);
          }
          return true;
        } else if (blockchain_status == TrnxBlockchainStatus.FAILED) {
          let trnxTypeW: string = "Withdraw";
          switch (tx_type) {
            case TrnxTypeEnum.DAPP:
              trnxTypeW = "Smart Contract Execution";
              break;
            case TrnxTypeEnum.APPROVE:
              trnxTypeW = "Approval";
              break;
            case TrnxTypeEnum.SWAP:
              trnxTypeW = "Swap";
              break;
            case TrnxTypeEnum.CROSS_CHAIN:
              trnxTypeW = "Cross chain Swap";
              break;
            default:
              break;
          }

          const notiMsg = trx_data?.coin.token_type === "TRC10" ? `Withdraw of NFT(${trx_data?.coin.coin_symbol.toUpperCase()}) with token id ${amount} has been confirmed.` : `${trnxTypeW} request of ${await Utility_Helper.bigNumberSafeConversion(amount)} ${trx_data?.coin.coin_symbol.toUpperCase()} has been failed.`;

          const notifData: any = {
            title: trnxTypeW.toUpperCase(),
            message: notiMsg,
            amount: amount,
            from_user_id: 0,
            to_user_id: user_id,
            wallet_address: from_address,
            tx_id: tx_row_id,
            coin_symbol: coin_symbol,
            coin_id: trx_data?.coin?.coin_id,
            coin_family: trx_data?.coin.coin_family,
            tx_type: tx_type,
            notification_type: trnxTypeW,
            state: TrnxBlockchainStatus.FAILED
          };
          Utility_Helper.SendNotification(notifData);
          return true;
        } else {
          return false;
        }
      } else {
        return false;
      }
    } else {
      return false;

    }
  }

}
export const tronTxStatusUpdateProcess = new TronTxStatusUpdateProcess();
