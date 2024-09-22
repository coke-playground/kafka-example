#include <atomic>
#include <csignal>
#include <string>
#include <iostream>

#include "kafka_awaiter.h"
#include "show_result.h"
#include "topic_manager.h"

#include "coke/sleep.h"
#include "coke/wait.h"
#include "coke/stop_token.h"
#include "coke/tools/option_parser.h"

using namespace protocol;

std::atomic<bool> running{true};

std::string offset_file;
std::string brokers;
int retry_max = 0;
bool latest = false;
long long offset_timestamp = -1;

void sig_handler(int signo) {
    if (running.load() == false)
        abort();

    running.store(false);
    running.notify_all();
}

WFKafkaTask *create_manual_fetch_task(WFKafkaClient &cli) {
    std::string query("api=fetch");

    auto *task = cli.create_kafka_task(query, 0, nullptr);

    KafkaConfig config;
    config.set_fetch_max_bytes(64 * 1024);
    config.set_fetch_timeout(1000);

    // 当手动设置的offset不在服务端的范围内时，会使用config中设置的这个值重新获取
    if (latest)
        config.set_offset_timestamp(KAFKA_TIMESTAMP_LATEST);
    else
        config.set_offset_timestamp(KAFKA_TIMESTAMP_EARLIEST);

    task->set_config(std::move(config));

    return task;
}

void add_toppars(WFKafkaTask *task, TopicManager &m) {
    m.for_each([task](const std::string &topic, int par, long long off) {
        KafkaToppar tp;
        tp.set_topic_partition(topic, par);

        if (off >= 0)
            tp.set_offset(off);
        else if (offset_timestamp > 0)
            tp.set_offset_timestamp(offset_timestamp);
        else if (latest)
            tp.set_offset_timestamp(KAFKA_TIMESTAMP_LATEST);
        else
            tp.set_offset_timestamp(KAFKA_TIMESTAMP_EARLIEST);

        task->add_toppar(tp);
    });
}

void update_toppars(const vec_records_t &vec_records, TopicManager &m) {
    for (const auto &records : vec_records) {
        if (!records.empty()) {
            KafkaRecord *rec = records.back();
            const char *topic = rec->get_topic();
            int partition = rec->get_partition();
            long long offset = rec->get_offset();

            // 这里维护的是下一个要被消费的offset
            m.update(topic, partition, offset + 1);
        }
    }
}

coke::Task<> manual_fetch(WFKafkaClient &cli, coke::StopToken &tk,
                          const std::string &offset_file)
{
    // 可以使用FinishGuard，在协程结束时自动调用tk.set_finished
    coke::StopToken::FinishGuard fg(&tk);
    TopicManager m;
    bool flag;

    flag = m.load(offset_file);
    if (!flag) {
        std::cerr << "Load offset from " << offset_file << " failed" << std::endl;
        co_return;
    }

    if (m.size() == 0) {
        std::cerr << "No topic in offset file" << std::endl;
        co_return;
    }

    while (!tk.stop_requested()) {
        WFKafkaTask *task = create_manual_fetch_task(cli);

        // 手动模式下需要自行维护和设置topic对应的偏移量
        add_toppars(task, m);

        co_await KafkaAwaiter(task);

        int state = task->get_state();
        int error = task->get_error();
        KafkaResult result;

        if (state == WFT_STATE_SUCCESS)
            result = std::move(*(task->get_result()));

        // 可以通过主动置空来提前提示task已不可用
        task = nullptr;

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

            // 拉取成功时维护新的偏移量
            update_toppars(vec_records, m);
        }
    }

    flag = m.dump(offset_file);
    if (!flag)
        std::cerr << "Dump offset to " << offset_file << " failed" << std::endl;
}

int main(int argc, char *argv[]) {
    coke::OptionParser args;

    args.add_string(offset_file, 'f', "offset-file", true)
        .set_description("The file to load and store offsets.")
        .set_long_descriptions({
            "The format of the file is one entry per line, each entry is",
            "topic partition next_offset",
        });

    args.add_string(brokers, 'b', "broker", true)
        .set_description("The url of broker(s), like \"kafka://localhost:9092/\".");

    args.add_integer(retry_max, 0, "retry", false)
        .set_default(0)
        .set_description("Max retry for each task.");

    args.add_integer(offset_timestamp, 'm', "offset-timestamp")
        .set_long_descriptions({
            "Use this timestamp if offset is negative in offset file,",
            "if a valid value is set, the --latest option is ignored."
        });

    args.add_flag(latest, 0, "latest")
        .set_description("Use latest offset if it is negative in offset file, default earlist");

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

    // 启动并分离协程
    coke::detach(manual_fetch(cli, tk, offset_file));

    // 等待并发送停止信号
    running.wait(true);
    tk.request_stop();

    // 等待后台协程完成，相当于join操作
    coke::sync_wait(tk.wait_finish());

    cli.deinit();
    return 0;
}
