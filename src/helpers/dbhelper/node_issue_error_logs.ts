import { NodeIssueErrorLogsModel } from "../../models/model";

class NodeIssueErrorLogQueries {

    public node_issue_error_logs: any = NodeIssueErrorLogsModel;

    public async node_issue_error_logs_create(obj: any) {
        try {
            return true;
        } catch (err: any) {
            console.error("Error in node_issue_error_logs_create>>", err)
            return true;
        }
    }

}

const node_issue_error_log_queries = new NodeIssueErrorLogQueries();
export default node_issue_error_log_queries;
