import { config } from "../config";
import { TrnxTypeEnum, Tron_contract_status } from "../enum";
import {
  Blockchain_Helper,
  Utility_Helper,
  kafkaHelper,
  node_issue_error_log_queries,
} from "../helpers";
import { blocks_trnx, transaction_data, Transaction } from "../interface/index";
import tronProcessHelper from "./processes.helper";

class queue_transactions {
  public trx_block_tx_topic = config.KAFKA_BLOCKS.TRX_BLOCK_TX_TOPIC;
  public trx_block_tx_group_id = config.KAFKA_BLOCKS.TRX_BLOCK_TX_GROUP_ID;
  public trx_behind_block_tx_topic =
    config.KAFKA_BLOCKS.TRX_BEHIND_BLOCK_TX_TOPIC;
  public trx_behind_block_tx_group =
    config.KAFKA_BLOCKS.TRX_BEHIND_BLOCK_TX_GROUP_ID;
  public deposit_withdraw_topic =
    config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;

  public tron_coin_family: any = config.STATIC_COIN_FAMILY.TRX;

  public startBlockTxQueue = async () => {
    let consumer = await kafkaHelper.kafka.consumer({
      groupId: this.trx_block_tx_group_id,
    });
    await consumer.subscribe({
      topic: this.trx_block_tx_topic,
      fromBeginning: true,
    });

    await consumer
      .run({
        eachMessage: ({
          topic,
          partition,
          message,
        }: {
          topic: any;
          partition: any;
          message: any;
        }) => {
          this.read_trnx(JSON.parse(message.value.toString()));
        },
      })
      .catch((err: Error) => {
        console.log("🔥 ~ startBlockTxQueue consumer error", err);
      });
  };

  public startBehindBlockTxQueue = async () => {
    let consumer = await kafkaHelper.kafka.consumer({
      groupId: this.trx_behind_block_tx_group,
    });
    await consumer.subscribe({
      topic: this.trx_behind_block_tx_topic,
      fromBeginning: true,
    });

    await consumer
      .run({
        eachMessage: ({
          topic,
          partition,
          message,
        }: {
          topic: any;
          partition: any;
          message: any;
        }) => {
          console.log("🚀 ~ startBehindBlockTxQueue topic", topic);
          console.log("🚀 ~ startBehindBlockTxQueue message.value", message.value.toString());
          this.read_trnx(JSON.parse(message.value.toString()));
        },
      })
      .catch((err: Error) => {
        console.log("🔥 ~ startBehindBlockTxQueue consumer error", err);
      });
  };

  public async read_trnx(data: transaction_data) {
    try {
      let is_token: any;

      if (data.status == "SUCCESS") {
        const transaction: Transaction = {
          tx_id: "",
          fromAddress: "",
          toAddress: "",
          token: null,
          amount: 0,
          blockId: 0,
          isNFTToken: false,
          txType: null,
          trnx_fee: 0
        };
        if (
          data?.contract?.length > 0 &&
          data?.contract[0]?.type ==
          Tron_contract_status.TransferContract
        ) {
          const amount: number = await Blockchain_Helper.tronWeb.fromSun(
            data?.contract[0]?.parameter.value.amount
          );
          transaction.tx_id = data.tx_id;
          transaction.fromAddress =
            await Blockchain_Helper.tronWeb.address.fromHex(
              data?.contract[0]?.parameter.value.owner_address
            );
          transaction.toAddress =
            await Blockchain_Helper.tronWeb.address.fromHex(
              data?.contract[0]?.parameter.value.to_address
            );
          transaction.amount = amount;
          transaction.blockId = data.block_number;
          await tron_tx_status_deposit_withdraw.add_trnx_to_queue(
            transaction
          );
        } else if (
          data?.contract?.length > 0 &&
          data?.contract[0]?.type ==
          Tron_contract_status.TriggerSmartContract
        ) {
          const contractAddress: any =
            await Blockchain_Helper.tronWeb.address.fromHex(
              data.contract[0]?.parameter.value
                .contract_address
            );

          is_token = await Blockchain_Helper.Trc20_Token(contractAddress);
          if (is_token) {
            const fromAddressHex = data.contract[0]?.parameter?.value?.owner_address;
            const fromAddress = await Blockchain_Helper.tronWeb.address.fromHex(fromAddressHex);

            const inputData = data.contract[0]?.parameter?.value?.data;
            const toAddressHex = '41' + inputData.substring(10 + 22, 10 + 62); // or input.substring(32, 72)
            const toAddress = await Blockchain_Helper.tronWeb.address.fromHex(toAddressHex);

            const amountHex = inputData.substring(72, 72 + 64);
            const amount: any = await Utility_Helper.bigNumberSafeMath(
              await Blockchain_Helper.tronWeb.toDecimal("0x" + amountHex),
              "/",
              is_token.decimals
            );
            transaction.token = is_token;
            transaction.tx_id = data.tx_id;
            transaction.fromAddress = fromAddress;
            transaction.toAddress = toAddress;
            transaction.amount = amount;
            transaction.blockId = data.block_number;

            await tron_tx_status_deposit_withdraw.add_trnx_to_queue(
              transaction
            );
          }
        }
      } else {
        // console.log("FAILED", data.tx_id);
      }
    } catch (err: any) {
      console.error("Error in read_trnx>>>", err);
      if (Number(data.retry_count) < 3) {
        data.retry_count = Number(data.retry_count) + 1;
        console.log(
          "Adding transaction in queue>>>",
          data.retry_count,
          "transaction id>>>",
          data
        );
        await kafkaHelper.produceMessage(this.trx_behind_block_tx_topic, data);
      }
      await node_issue_error_log_queries.node_issue_error_logs_create({
        function: "read_trnx",
        block_number: data.block_number.toString(),
        error: err.message,
        transaction_id: data.tx_id,
        from_adrs: null,
        to_adrs: null,
        coin_family: config.STATIC_COIN_FAMILY.TRX,
        extra: "catch under read_trnx",
      });
    }
  }
  public async add_trnx_to_queue(trnx: Transaction) {
    try {
      let ourTrnx: any;
      let Addressdeposit: any =
        await tronProcessHelper.check_our_wallet_address(trnx.toAddress);
      if (Addressdeposit) {
        console.log("out deposit address>>>", Addressdeposit);
        ourTrnx = trnx;
        ourTrnx.txType = TrnxTypeEnum.DEPOSIT;
      }
      let AddressWithdraw: any =
        await tronProcessHelper.check_our_wallet_address(trnx.fromAddress);
      if (AddressWithdraw) {
        console.log("out withdraw address>>>", AddressWithdraw);
        ourTrnx = trnx;
        ourTrnx.txType = TrnxTypeEnum.WITHDRAW;
      }
      if (ourTrnx) {
        await kafkaHelper.produceMessage(this.deposit_withdraw_topic, ourTrnx);
      }
    } catch (err: any) {
      console.error("Error in add_trnx_to_que>>>", err);
    }
  }
}
export const tron_tx_status_deposit_withdraw = new queue_transactions();
