#include <gperftools/profiler.h>
#include "../include/skiplist/skiplist.h"

using namespace ::tiny_lsm;

// 测试跳表的插入性能
void test_skiplist_insertion_performance() {
  SkipList skipList;
  const int num_elements = 5000;

  // 插入大量数据
  for (int i = 0; i < num_elements; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    skipList.put(key, value, 0);
  }

  // 验证插入的数据
  for (int i = 0; i < num_elements; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string expected_value = "value" + std::to_string(i);
    skipList.get(key, 0).get_value();
  }
}
int main() {
  // 启动性能分析器
  ProfilerStart("skiplist_insertion.pprof");
  test_skiplist_insertion_performance();
  // 停止性能分析器
  ProfilerStop();
}