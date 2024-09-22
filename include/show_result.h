#ifndef KAFKA_EXAMPLE_SHOW_RESULT_H
#define KAFKA_EXAMPLE_SHOW_RESULT_H

#include <format>
#include <string>
#include <iostream>

#include "workflow/WFKafkaClient.h"

using vec_records_t = std::vector<std::vector<protocol::KafkaRecord *>>;

inline void
show_kafka_result(const vec_records_t &vec_records) {
    std::string delimiter(80, '-');

    for (const auto &records : vec_records) {
        for (const auto &record : records) {
            const char *topic = record->get_topic();
            int partition = record->get_partition();
            long long offset = record->get_offset();
            long long timestamp = record->get_timestamp();
            const void *value;
            std::size_t value_len;

            record->get_value(&value, &value_len);
            auto str = std::format("topic:{} partition:{} offset:{} timestamp:{} vlen:{}\n",
                                   topic, partition, offset, timestamp, value_len);
            std::cout << str;
        }
        std::cout << delimiter << std::endl;
    }
}

#endif // KAFKA_EXAMPLE_SHOW_RESULT_H
