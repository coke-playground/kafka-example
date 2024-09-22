#include <atomic>
#include <csignal>
#include <format>
#include <string>
#include <iostream>

#include "kafka_awaiter.h"
#include "show_result.h"

#include "coke/sleep.h"
#include "coke/wait.h"
#include "coke/tools/option_parser.h"

using namespace protocol;

std::atomic<bool> running{true};

std::string brokers;
std::string topic;
int retry_max = 0;

void sig_handler(int signo) {
    if (running.load() == false)
        abort();

    running.store(false);
}

KafkaResultAwaiter produce_message(WFKafkaClient &cli) {
    std::string query("api=produce");
    auto *task = cli.create_kafka_task(query, retry_max, nullptr);

    KafkaConfig cfg;
    cfg.set_produce_timeout(1000);
    task->set_config(cfg);

    for (int i = 0; i < 20; i++) {
        KafkaRecord r;
        std::string value = "kafka-value-" + std::to_string(i);

        r.set_value(value.c_str(), value.size());
        task->add_produce_record(topic, -1, std::move(r));
    }

    return KafkaResultAwaiter(task);
}

coke::Task<> produce(WFKafkaClient &cli) {
    while (running.load()) {
        // 使用返回结果的等待器，可以避免task生命周期带来的问题，但会带来额外的拷贝或移动
        // 开销，对于结果中未包含的内容(例如task->get_kafka_error())，则无法获取到
        KafkaWaitResult res = co_await produce_message(cli);

        int state = res.state;
        int error = res.error;

        if (state != WFT_STATE_SUCCESS) {
            auto str = std::format("Produce Failed state:{} error:{}", state, error);
            std::cout << str << std::endl;
        }
        else {
            std::cout << "Produce Success" << std::endl;

            std::vector<std::vector<KafkaRecord *>> vec_records;
            res.result.fetch_records(vec_records);

            show_kafka_result(vec_records);
        }

        co_await coke::sleep(1.0);
    }
}

int main(int argc, char *argv[]) {
    coke::OptionParser args;

    args.add_string(brokers, 'b', "broker", true)
        .set_description("The url of broker(s), like \"kafka://localhost:9092/\".");

    args.add_string(topic, 't', "topic", true)
        .set_description("The topic to produce to.");

    args.add_integer(retry_max, 0, "retry", false)
        .set_default(0)
        .set_description("Max retry for each task.");

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

    WFKafkaClient cli;
    cli.init(brokers);

    // 可以简单地同步等待，当收到停止信号后，协程可能因正在sleep而延迟退出
    coke::sync_wait(produce(cli));

    cli.deinit();
    return 0;
}
