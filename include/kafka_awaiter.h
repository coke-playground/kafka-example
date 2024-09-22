#ifndef KAFKA_EXAMPLE_KAFKA_AWAITER_H
#define KAFKA_EXAMPLE_KAFKA_AWAITER_H

#include "coke/basic_awaiter.h"
#include "workflow/WFKafkaClient.h"

/**
 * 与WFHttpTask等任务不同，WFKafkaTask不是WFNetworkTask的实例化，因此无法通过
 * coke::NetworkAwaiter进行异步等待。这里展示如何通过coke提供的基础组件创建一个
 * 自定义的等待器。
*/

class KafkaAwaiter : public coke::BasicAwaiter<void> {
public:
    KafkaAwaiter(WFKafkaTask *task) {
        // 该方式要求任务有回调机制，通过回调函数唤醒等待器
        task->set_callback([info = this->get_info()](WFKafkaTask *) {
            KafkaAwaiter *awaiter = info->get_awaiter<KafkaAwaiter>();
            awaiter->done();
        });

        // 将这个任务移交给等待器处理
        this->set_task(task);
    }
};

/**
 * 对于支持移动操作的结果类型，可以创建一个保存结果的结构体，一次性返回所有需要的内容。
 * 这可以避免在co_await返回后仍需对task进行操作的问题，但会引入额外的移动构造开销；
 * 在某些场景下可能只需操作结果的部分内容，但由于无法预料，在这里也需要全部保存下来，
 * 这也带来了一定的时间和空间开销。
*/

struct KafkaWaitResult {
    int state;
    int error;
    protocol::KafkaResult result;
};

class KafkaResultAwaiter : public coke::BasicAwaiter<KafkaWaitResult> {
public:
    KafkaResultAwaiter(WFKafkaTask *task) {
        task->set_callback([info = this->get_info()](WFKafkaTask *task) {
            KafkaResultAwaiter *awaiter = info->get_awaiter<KafkaResultAwaiter>();
            int state = task->get_state();
            int error = task->get_error();
            auto *result = task->get_result();

            awaiter->emplace_result(state, error, std::move(*result));
            awaiter->done();
        });

        // 将这个任务移交给等待器处理
        this->set_task(task);
    }
};

#endif // KAFKA_EXAMPLE_KAFKA_AWAITER_H
