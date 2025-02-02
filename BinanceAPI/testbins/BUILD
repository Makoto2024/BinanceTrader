load("@rules_go//go:def.bzl", "go_binary")

go_binary(
  name = "get_historical_klines_main",
  srcs = ["gethistoricalklines.go"],
  deps = [
    "//BinanceAPI/common:common",
    "//BinanceAPI/klines:klines",
    "//BinanceAPI/storage:storage",
  ],
  visibility = ["//visibility:public"],
)