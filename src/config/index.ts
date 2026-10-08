import zookeeperConfig from "../connections/zookeeper.conn";
import { config } from "./config";
import { assertRequiredSecrets, REQUIRED_SECRETS } from "./required-secrets";

assertRequiredSecrets(zookeeperConfig.config, REQUIRED_SECRETS);

export { config }
