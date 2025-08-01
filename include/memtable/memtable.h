#pragma once

#include <cstddef>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include "../iterator/iterator.h"
#include "../skiplist/skiplist.h"

namespace tiny_lsm {

class BlockCache;
class SST;
class SSTBuilder;
class TranContext;

class MemTable {
  friend class TranContext;
  friend class HeapIterator;

 private:
  void put_(const std::string &key, const std::string &value, uint64_t tranc_id);

  SkipListIterator get_(const std::string &key, uint64_t tranc_id);
  // 在活跃表中查找key，无锁
  SkipListIterator cur_get_(const std::string &key, uint64_t tranc_id);
  // 在冻结表中查找key，无锁
  SkipListIterator frozen_get_(const std::string &key, uint64_t tranc_id);

  void remove_(const std::string &key, uint64_t tranc_id);
  void frozen_cur_table_();  // _ 表示不需要锁的版本

 public:
  MemTable();
  ~MemTable();

  void put(const std::string &key, const std::string &value, uint64_t tranc_id);
  void put_batch(const std::vector<std::pair<std::string, std::string>> &kvs, uint64_t tranc_id);

  SkipListIterator get(const std::string &key, uint64_t tranc_id);
  std::vector<std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>> get_batch(
      const std::vector<std::string> &keys, uint64_t tranc_id);
  void remove(const std::string &key, uint64_t tranc_id);
  void remove_batch(const std::vector<std::string> &keys, uint64_t tranc_id);

  void clear();
  // 当MemTable中的数据量达到阈值时, 会调用这个函数将最古老的一个SST进行持久化, 形成一个Level 0的SST
  std::shared_ptr<SST> flush_last(SSTBuilder &builder, std::string &sst_path, size_t sst_id,
                                  std::shared_ptr<BlockCache> block_cache);
  void frozen_cur_table();
  // 获取当前活跃跳表的大小
  size_t get_cur_size();
  size_t get_frozen_size();
  size_t get_total_size();
  HeapIterator begin(uint64_t tranc_id);
  HeapIterator end();
  HeapIterator iters_preffix(const std::string &preffix, uint64_t tranc_id);

  std::optional<std::pair<HeapIterator, HeapIterator>> iters_monotony_predicate(
      uint64_t tranc_id, std::function<int(const std::string &)> predicate);

      void print_memtable();
 private:
  std::shared_ptr<SkipList> current_table;
  // 冻结的memtable，最新的SkipList在head,由新到旧
  std::list<std::shared_ptr<SkipList>> frozen_tables;
  // 已被冻结的内存大小
  size_t frozen_bytes;
  // 冻结表的锁
  std::shared_mutex frozen_mtx;
  // 活跃表的锁
  std::shared_mutex cur_mtx;
};
}  // namespace tiny_lsm