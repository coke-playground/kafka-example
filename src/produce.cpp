#include <atomic>
#include <csignal>
#include <format>
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
int retry_max = 0;

void sig_handler(int signo) {
    if (running.load() == false)
        abort();

    running.store(false);
    running.notify_all();
}

WFKafkaTask *create_produce_task(WFKafkaClient &cli) {
    std::string query("api=produce");
    auto *task = cli.create_kafka_task(query, retry_max, nullptr);

    KafkaConfig cfg;
    cfg.set_produce_timeout(1000);
    task->set_config(cfg);

    return task;
}

coke::Task<> produce(WFKafkaClient &cli, coke::StopToken &tk) {
    // 循环执行，直到收到停止信号
    while (!tk.stop_requested()) {
        WFKafkaTask *task = create_produce_task(cli);

        // 每次生产一批数据
        for (int i = 0; i < 20; i++) {
            KafkaRecord r;
            std::string value = "kafka-value-" + std::to_string(i);

            r.set_value(value.c_str(), value.size());

            // 生产时可以为这个KafkaRecord指定partition，
            // 也可以指定-1以使用用户设置的`partitioner`来判定要生产到哪个partition，
            // 若未设置`partitioner`则随机指定partition
            task->add_produce_record(topic, -1, std::move(r));
        }

        // 这里使用了一个简单的等待器，不会返回任何内容，需要自行从task中获取请求结果，
        // 但注意task的生命周期仅维持到下一个`co_await`发生前，这是workflow协程化
        // 过程中最令人难以接受的一件事。
        co_await KafkaAwaiter(task);

        int state = task->get_state();
        int error = task->get_error();

        if (state != WFT_STATE_SUCCESS) {
            auto str = std::format("Produce Failed state:{} error:{}", state, error);
            std::cout << str << std::endl;
        }
        else {
            std::cout << "Produce Success" << std::endl;

            std::vector<std::vector<KafkaRecord *>> vec_records;
            KafkaResult result = std::move(*(task->get_result()));
            result.fetch_records(vec_records);

            show_kafka_result(vec_records);
        }

        // 每次生产后等待一下，限制生产速度
        co_await tk.wait_stop_for(std::chrono::seconds(1));

        // task->get_xxx() // 不好！task已然不存在了
    }

    // 发出任务完成的通知
    tk.set_finished();
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

    coke::StopToken tk;
    WFKafkaClient cli;
    cli.init(brokers);

    // 启动并分离produce协程
    coke::detach(produce(cli, tk));

    // 等待并发送停止信号
    running.wait(true);
    tk.request_stop();

    // 等待后台协程完成，相当于join操作
    coke::sync_wait(tk.wait_finish());

    cli.deinit();
    return 0;
}
