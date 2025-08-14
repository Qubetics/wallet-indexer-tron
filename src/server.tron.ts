import { TronProcessBlocks, tronDepositWithdrawProcess, ethPendingWithdrawalProcess, tronTxStatusUpdateProcess, tron_tx_status_deposit_withdraw, updateBalanceProcess } from "./processes"
import schedule from "node-schedule";


class TRONStartProcess {
   constructor() {
      // this.testing_node();
      this.readBlocksProcess();
      this.consume_read_block_queue();
      this.readBehindBlocksProcess();
      this.readSpecificBlock();
      this.ethDepositProcess();
      this.getPendingWithdrawalTxProcess();
      this.updateTxStatusProcess();
      this.updateBalance();
      this.updateAllCoinsBalance();
      // this.balanceUpdateByWallet();
   }

   // public testing_node() {
   //    schedule.scheduleJob("*/2 * * * * *", async function () {
   //       await TronProcessBlocks.testingNode();
   //    });
   // }
   public readBlocksProcess() {
      schedule.scheduleJob("*/1 * * * * *", async function () {
         await TronProcessBlocks.getBlocks();
      });
   }
   public consume_read_block_queue() {
      setTimeout(async () => {
         await tron_tx_status_deposit_withdraw.startBlockTxQueue();
         await tron_tx_status_deposit_withdraw.startBehindBlockTxQueue();
      }, 10000);
   }
   public async readBehindBlocksProcess() {
      schedule.scheduleJob("*/5 * * * * *", async function () {
         await TronProcessBlocks.readBehindBlock();
      })
   }
   public readSpecificBlock() {
      schedule.scheduleJob("*/2 * * * *", async function () {
         await TronProcessBlocks.readSpecificBlock();
      })
   }
   public async ethDepositProcess() {
      setTimeout(async () => {
         await tronDepositWithdrawProcess.startDepositQueue();
      }, 10000);
   }
   public getPendingWithdrawalTxProcess() {
      schedule.scheduleJob("*/10 * * * * *", async function () {
         await ethPendingWithdrawalProcess.getTransactionFromDB();
      });
   }
   public async updateTxStatusProcess() {
      setTimeout(async () => {
         await tronTxStatusUpdateProcess.startTxStatusUpdateQueue();
      }, 10000);
   }
   public updateBalance() {
      setTimeout(async () => {
         await updateBalanceProcess.startUpdateBalance()
      }, 10000)
   }
   public updateAllCoinsBalance() {
      setTimeout(async () => {
         await updateBalanceProcess.startUpdateAllBalances()
      }, 10000)
   }

   public async balanceUpdateByWallet() {
      schedule.scheduleJob("*/1 * * * *", async function () {
         await updateBalanceProcess.updateWalletBalance();
      });
   }
}

new TRONStartProcess();

