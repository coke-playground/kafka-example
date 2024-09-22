#include <atomic>
#include <csignal>
#include <string>
#include <iostream>

#include "kafka_awaiter.h"
#include "show_result.h"

#include "coke/sleep.h"
#include "coke/wait.h"
#include "coke/stop_token.h"
#include "coke/tools/option_parser.h"

using namespace protocol;

std::atomic<bool> running{true};

std::string brokers;
std::string topic;
std::string group;
int retry_max = 0;
bool latest = false;

void sig_handler(int signo) {
    if (running.load() == false)
        abort();

    running.store(false);
    running.notify_all();
}

WFKafkaTask *create_group_fetch_task(WFKafkaClient &cli) {
    std::string query;
    query.append("api=fetch&topic=").append(topic);

    auto *task = cli.create_kafka_task(query, retry_max, nullptr);
    long long t = latest ? KAFKA_TIMESTAMP_LATEST : KAFKA_TIMESTAMP_EARLIEST;
    KafkaConfig config;

    // offset_timestamp用于指定当group中没有记录偏移量时如何消费数据；
    // latest表示从最新的数据开始消费，earlist表示从目前最早的数据开始消费；
    config.set_offset_timestamp(t);

    // 消费请求发送到broker后，若没有足够的数据用于返回，则会至多等待fetch_timeout毫秒，
    // 直到有足够的数据或时间耗尽。
    config.set_fetch_timeout(1000);

    // 指定单次消费消息的最大字节数
    config.set_fetch_max_bytes(64 * 1024);

    task->set_config(std::move(config));

    return task;
}

WFKafkaTask *create_commit_task(WFKafkaClient &cli, const vec_records_t &vec_records) {
    WFKafkaTask *task = cli.create_kafka_task("api=commit", retry_max, nullptr);
    bool has_data = false;

    for (const auto &records : vec_records) {
        if (!records.empty()) {
            has_data = true;
            task->add_commit_record(*records.back());
        }
    }

    if (!has_data) {
        task->dismiss();
        task = nullptr;
    }

    return task;
}

coke::Task<> group_fetch(WFKafkaClient &cli, coke::StopToken &tk) {
    // 可以使用FinishGuard，在协程结束时自动调用tk.set_finished
    coke::StopToken::FinishGuard fg(&tk);

    while (!tk.stop_requested()) {
        int state, error;
        KafkaResult result;

        // 通过在一个代码块中将所需数据全部取出的方式，避免task的生命周期在下一个`co_await`
        // 处终止带来的额外负担。虽然不完美，但确实可以解决问题。
        {
            WFKafkaTask *task = create_group_fetch_task(cli);
            co_await KafkaAwaiter(task);

            state = task->get_state();
            error = task->get_error();

            if (state == WFT_STATE_SUCCESS)
                result = std::move(*(task->get_result()));
        }

        if (state != WFT_STATE_SUCCESS) {
            auto str = std::format("Fetch Failed state:{} error:{}", state, error);
            std::cout << str << std::endl;

            // 拉取失败时可能服务端或网络故障，可以暂停一段时间。
            co_await tk.wait_stop_for(std::chrono::seconds(1));
        }
        else {
            std::cout << "Fetch Success" << std::endl;

            std::vector<std::vector<KafkaRecord *>> vec_records;
            result.fetch_records(vec_records);

            show_kafka_result(vec_records);

            // group模式拉取到数据后需要手动提交offset，以便下次消费可以从上次结束的位置开始
            WFKafkaTask *commit_task = create_commit_task(cli, vec_records);
            if (commit_task) {
                co_await KafkaAwaiter(commit_task);

                state = commit_task->get_state();
                if (state == WFT_STATE_SUCCESS)
                    std::cout << "Commit Success" << std::endl;
                else
                    std::cout << "Commit Failed" << std::endl;
            }
            else {
                std::cout << "No commit data" << std::endl;
            }
        }
    }

    // 工作结束前，主动退出当前消费组，若该组有其他消费者，会及时触发rebalance
    WFKafkaTask *leave_task = cli.create_leavegroup_task(retry_max, nullptr);
    co_await KafkaAwaiter(leave_task);

    int state = leave_task->get_state();
    if (state == WFT_STATE_SUCCESS)
        std::cout << "Leave Success" << std::endl;
    else
        std::cout << "Leave Failed" << std::endl;
}

int main(int argc, char *argv[]) {
    coke::OptionParser args;

    args.add_string(brokers, 'b', "broker", true)
        .set_description("The url of broker(s), like \"kafka://localhost:9092/\".");

    args.add_string(topic, 't', "topic", true)
        .set_description("The topic to fetch from.");

    args.add_string(group, 'g', "group", true)
        .set_description("The name of fetch group.");

    args.add_integer(retry_max, 0, "retry", false)
        .set_default(0)
        .set_description("Max retry for each task.");

    args.add_bool(latest, 0, "latest")
        .set_default(false)
        .set_description("Use latest offset if no committed offset, default earlist");

    args.set_help_flag('h', "help");

    std::string err;
    int ret = args.parse(argc, argv, err);

    if (ret < 0) {
        std::cerr << err << std::endl;
        return 1;
    }
    else if (ret > 0) {
        args.usage(std::cout);
        return 0;
    }

    signal(SIGINT, sig_handler);

    coke::StopToken tk;
    WFKafkaClient cli;
    cli.init(brokers, group);

    // 启动并分离协程
    coke::detach(group_fetch(cli, tk));

    // 等待并发送停止信号
    running.wait(true);
    tk.request_stop();

    // 等待后台协程完成，相当于join操作
    coke::sync_wait(tk.wait_finish());

    cli.deinit();
    return 0;
}
