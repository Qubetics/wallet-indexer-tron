import { config } from '../config/config';
import axios from "axios";
class UTXO {
    public config: any;

    constructor() {
        this.config = {
            method: 'get',
            headers: {
                'apikey': `${config.NODE.BTC_API_KEY}`,
                'Content-Type': 'application/json',
            }
        };
    }


    public async getUserBtcBalance(address: string) {
        let bal_data: { status: boolean, balance: number } = { status: false, balance: 0 };
        try {
            this.config.url = `${config.NODE.BTC_RPC_URL}api/v2/address/${address}`
            const response = await axios(this.config);
            let tokBalance: any = response.data.balance / 100000000;
            return { status: true, balance: Number(tokBalance) };
        } catch (error) {
            console.error('error in getUserBtcBalance >>>>>>', error);
            return bal_data;
        }
    }
}

var utxo = new UTXO();
export default utxo;
