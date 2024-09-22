package(default_visibility = ["//visibility:public"])

cc_library(
    name = "kafka_helper",
    srcs = [],
    hdrs = [
        "include/kafka_awaiter.h",
        "include/show_result.h",
        "include/topic_manager.h",
    ],
    includes = ["include"],
    deps = [
        "@coke//:common",
        "@workflow//:kafka",
    ]
)

cc_binary(
    name = "produce",
    srcs = ["src/produce.cpp"],
    deps = [
        "//:kafka_helper",
        "@coke//:tools",
    ]
)

cc_binary(
    name = "group_fetch",
    srcs = ["src/group_fetch.cpp"],
    deps = [
        "//:kafka_helper",
        "@coke//:tools",
    ]
)

cc_binary(
    name = "manual_fetch",
    srcs = ["src/manual_fetch.cpp"],
    deps = [
        "//:kafka_helper",
        "@coke//:tools",
    ]
)

cc_binary(
    name = "result_awaiter",
    srcs = ["src/result_awaiter.cpp"],
    deps = [
        "//:kafka_helper",
        "@coke//:tools",
    ]
)
