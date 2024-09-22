#ifndef KAFKA_EXAMPLE_TOPIC_MANAGER_H
#define KAFKA_EXAMPLE_TOPIC_MANAGER_H

#include <fstream>
#include <string>
#include <map>

class TopicManager {
public:
    struct TopparKey {
        std::string topic;
        int partition;

        bool operator< (const TopparKey &other) const {
            int cmp = this->topic.compare(other.topic);
            if (cmp == 0)
                return this->partition < other.partition;
            else return cmp < 0;
        }
    };

    using OffsetMap = std::map<TopparKey, long long>;

    TopicManager() = default;
    ~TopicManager() = default;

    bool load(const std::string &offset_file) {
        std::ifstream ifs(offset_file);
        std::string topic;
        int partition;
        long long offset;

        if (!ifs.good())
            return false;

        while (ifs >> topic >> partition >> offset)
            m.emplace(TopparKey{topic, partition}, offset);

        return ifs.eof();
    }

    bool dump(const std::string &offset_file) {
        std::ofstream ofs(offset_file);

        if (!ofs.good())
            return false;

        for (auto &[tp, off] : m)
            ofs << tp.topic << ' ' << tp.partition << ' ' << off << std::endl;

        return ofs.good();
    }

    bool update(const std::string &topic, int partition, long long offset) {
        auto it = m.find(TopparKey{topic, partition});
        if (it != m.end()) {
            if (it->second < offset)
                it->second = offset;
            return true;
        }

        return false;
    }

    bool add(const std::string &topic, int partition, long long offset) {
        TopparKey key{topic, partition};
        auto it = m.find(key);
        if (it == m.end()) {
            m.emplace(std::move(key), offset);
            return true;
        }

        return false;
    }

    template<typename Func>
    void for_each(Func func) {
        for (auto &[tp, off] : m)
            func(tp.topic, tp.partition, off);
    }

    std::size_t size() const {
        return m.size();
    }

private:
    OffsetMap m;
};

#endif // KAFKA_EXAMPLE_TOPIC_MANAGER_H
