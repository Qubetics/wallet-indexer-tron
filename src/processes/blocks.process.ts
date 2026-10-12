import { config } from "../config/index";
import {
  kafkaHelper,
  Blockchain_Helper,
  node_issue_error_log_queries,
} from "../helpers/index";
import { transaction_data } from "../interface/index";
import { TronOldBLockModel } from "../models/model";
import tronProcessHelper from "./processes.helper";
import { KEYS } from "../enum/index";

class Process_Blocks {
  public minBlockConfirmation: number = config.MIN_BLOCK_CONFIRMATIONS_TRON;
  public tron_blocks: string = config.REDISKEYS.TRON_BLOCKS;
  public old_tron_block: string = config.REDISKEYS.OLD_BLOCK_TRON;
  public new_old_block: string = config.REDISKEYS.NEW_BLOCK_TRON;
  public tron_coin_family: any = config.STATIC_COIN_FAMILY.TRX;
  public trx_block_tx_topic = config.KAFKA_BLOCKS.TRX_BLOCK_TX_TOPIC;
  public trx_behind_block_tx_topic =
    config.KAFKA_BLOCKS.TRX_BEHIND_BLOCK_TX_TOPIC;
  public behined_blocks: string = config.READ_BEHINED_BLOCK_TRON;
  public blocks: any = config.BLOCKS;
  public all_processes: string = config.REDISKEYS.ALL_PROCESSES;
  public node_alert_tron: string = config.REDISKEYS.NODE_ALERT_TRON;
  public specific_block: string = config.REDISKEYS.SPECIFIC_BLOCK_TRON;

  constructor() { }

  public async getBlocks() {
    try {
      // Get block number (from and to from Redis)
      let old_block: any = await tronProcessHelper.getKeyValuePair(
        this.tron_blocks,
        this.old_tron_block
      );
      let new_block: any = await tronProcessHelper.getKeyValuePair(
        this.tron_blocks,
        this.new_old_block
      );

      if (!old_block) {
        console.log("No block present in old block");
        await tronProcessHelper.setBlocks(
          this.tron_blocks,
          this.old_tron_block,
          this.minBlockConfirmation,
          null
        );
      }
      if (!new_block) {
        console.log("No block present in new block");
        await tronProcessHelper.setBlocks(
          this.tron_blocks,
          this.new_old_block,
          this.minBlockConfirmation,
          null
        );
      }

      if (old_block && new_block) {
        let oldBlock: number = Number(old_block);
        let newBlock = Number(new_block);
        if (newBlock <= oldBlock) {
          await tronProcessHelper.setBlocks(
            this.tron_blocks,
            this.new_old_block,
            this.minBlockConfirmation,
            newBlock
          );
        } else {
          let block_diff: number = newBlock - oldBlock;
          if (block_diff > 100) {
            await tronProcessHelper.diffence_blocks_add_in_db(
              newBlock,
              oldBlock
            );
            await tronProcessHelper.setKeyValuePair(
              this.tron_blocks,
              this.old_tron_block,
              newBlock.toString()
            );
            await tronProcessHelper.setBlocks(
              this.tron_blocks,
              this.new_old_block,
              this.minBlockConfirmation,
              newBlock
            );
          } else {
            await this.readBlocks(oldBlock, newBlock);
          }
        }
      } else {
        console.log(" no old block and no new block");
      }
    } catch (err: any) {
      console.error("Error in getBlocks", err);
      await node_issue_error_log_queries.node_issue_error_logs_create({
        function: "getBlocks",
        block_number: null,
        error: err.message,
        transaction_id: null,
        from_adrs: null,
        to_adrs: null,
        coin_family: this.tron_coin_family,
        extra: "catch under getBlocks",
      });
    }
  }
  public async readBlocks(blockNumber: number, toBlockNumber: number) {
    try {
      console.debug(
        `------------------readBlocks---------------------`,
        blockNumber
      );

      let block_updated: number = blockNumber + 1;
      await tronProcessHelper.setKeyValuePair(
        this.tron_blocks,
        this.old_tron_block,
        block_updated.toString()
      );
      console.table({
        LATEST_BLOCK: blockNumber,
        BLOCK_DIFFERENCE: toBlockNumber - blockNumber,
        TO_BLOCK_NUMBER: toBlockNumber,
      });
      this.getTxFromBlock(blockNumber, KEYS.latestBlock);
    } catch (err: any) {
      console.error("Error in readBlocks:", (err as Error).message);
    }
  }
  public async getTxFromBlock(block_number: number, key: string) {
    try {
      const getBlock = await Blockchain_Helper.tronWeb.trx.getBlock(
        block_number
      );

      const transactions: any = getBlock.transactions;

      if (!transactions) return;

      for await (let transaction of transactions) {
        const transaction_data: transaction_data = {
          status: transaction?.ret[0]?.contractRet == "SUCCESS" ? 'SUCCESS' : 'FAILED',
          contract: transaction.raw_data?.contract ? transaction.raw_data?.contract : [],
          block_number: block_number,
          retry_count: 0,
          tx_id: transaction.txID,
          key: null,
          pair: null,
        }

        if (key == KEYS.latestBlock) {
          transaction_data.key = "latest";
          await kafkaHelper.produceMessage(this.trx_block_tx_topic, transaction_data);
        } else {
          transaction_data.key = "behined & specific";
          await kafkaHelper.produceMessage(
            this.trx_behind_block_tx_topic,
            transaction_data
          );
        }
      }
    } catch (err: any) {
      console.error(`error in getTxFromBlock error >>>`, err);
      await TronOldBLockModel.create({
        block_number: block_number,
        coin_family: this.tron_coin_family,
        start_block: block_number,
        end_block: block_number,
        status: 0,
      });
      await node_issue_error_log_queries.node_issue_error_logs_create({
        function: "getTxFromBlock",
        block_number: block_number.toString(),
        error: err.message,
        transaction_id: null,
        from_adrs: null,
        to_adrs: null,
        coin_family: this.tron_coin_family,
        extra: "catch under getTxFromBlock",
      });
    }
  }
  public async readBehindBlock() {
    try {
      // REDIS BASED
      let data_exist: any = await tronProcessHelper.getKeyValuePair(
        this.blocks,
        this.behined_blocks
      );
      let data_exists = JSON.parse(data_exist);
      if (data_exists) {
        console.log("This block exist in redis read behined");

        let id = data_exists.id;
        let end_block = data_exists.end_block;
        let start_block = data_exists.start_block;

        if (end_block > start_block) {
          console.log("This block go for getTxFromBlock");

          await this.getTxFromBlock(start_block, KEYS.behinedBlock);
          start_block = start_block + 1;
          let add_data = {
            id: id,
            start_block: start_block,
            end_block: end_block,
            diff: end_block - start_block,
          };
          await tronProcessHelper.setKeyValuePair(
            this.blocks,
            this.behined_blocks,
            JSON.stringify(add_data)
          );
        } else {
          console.log("id>>>>>>>>>>>", id);
          await TronOldBLockModel.update({ status: 1 }, { where: { id: id } });
          await tronProcessHelper.deleteKeyValuePair(
            this.blocks,
            this.behined_blocks
          );
          await tronProcessHelper.behindBlocks();
        }
      } else {
        await tronProcessHelper.behindBlocks();
      }
    } catch (err: any) {
      console.error("Error in readBehindBlock>>>>", err);
    }
  }
  public async readSpecificBlock() {
    try {
      console.log("Entere into readSpecificBlock>>>");
      let specificBlocks: any = await tronProcessHelper.getKeyValuePair(
        this.all_processes,
        this.specific_block
      );

      let specific_block: any = JSON.parse(specificBlocks);

      if (specific_block && Number(specific_block) > 0) {
        console.table({ SPECIFIC_BLOCK: specific_block });
        await this.getTxFromBlock(specific_block, KEYS.readSpecificBlock);
        await tronProcessHelper.setKeyValuePair(
          this.all_processes,
          this.specific_block,
          "0"
        );
      } else {
        console.log("Specific block is not present>>>>");
        await tronProcessHelper.setKeyValuePair(
          this.all_processes,
          this.specific_block,
          "0"
        );
      }
    } catch (err: any) {
      console.error("Error in readSpecificBlock>>>", err);
    }
  }
}
export let TronProcessBlocks = new Process_Blocks();
