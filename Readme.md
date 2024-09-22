# Kafka Example
这个项目展示了将Workflow Kafka生产和消费任务协程化的方法，有以下几个示例

1. Produce
    展示了向指定的broker和topic生产数据的方法。
2. Group Fetch
    使用消费者组模式消费数据，在收到数据后手动提交offset，并在工作结束时主动退出group。
3. Manual Fetch
    使用手动模式消费数据，这需要手动维护topic, partition的offset信息。
4. Result Awaiter
    带有返回值的等待器示例。

## 构建环境
GCC >= 13

## LICENSE
Apache 2.0
