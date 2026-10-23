import { DeviceTokenModel } from "../../models/model";

class DeviceTokenQueries {

    public device_tokens: any = DeviceTokenModel;

    public async device_tokens_with_limit(attr: any, where_clause: any, order: any) {
        try {
            let data: any = await this.device_tokens.findAll({
                attributes: attr,
                where: where_clause,
                order: order,
                raw: true

            })
            return data;
        } catch (err: any) {
            console.error(`Error in device_tokens_with_limit>>>`, err);
            return false;
        }
    }

}

const device_token_queries = new DeviceTokenQueries();
export default device_token_queries;
