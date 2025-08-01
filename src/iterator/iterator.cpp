#include "../../include/iterator/iterator.h"
#include <memory>
#include <tuple>
#include <vector>

namespace tiny_lsm {

// *************************** SearchItem ***************************
// 比较规则：按 key 升序，tranc_id 降序, idx 升序
bool operator<(const SearchItem &a, const SearchItem &b) {
  // TODO: Lab2.2 实现比较规则
  if (a.key_ != b.key_) {
    return a.key_ < b.key_;
  }
  // TODO: 暂时忽略 tranc_id 和level 的比较
  return a.idx_ < b.idx_;
}

bool operator>(const SearchItem &a, const SearchItem &b) {
  // TODO: Lab2.2 实现比较规则
  if (a.key_ != b.key_) {
    return a.key_ > b.key_;
  }
  return a.idx_ > b.idx_;
}

bool operator==(const SearchItem &a, const SearchItem &b) {
  // TODO: Lab2.2 实现比较规则
  return a.key_ == b.key_ && a.idx_ == b.idx_;
}

// *************************** HeapIterator ***************************
// TODO: 考虑后续是否可以传引用
HeapIterator::HeapIterator(std::vector<SearchItem> item_vec, uint64_t max_tranc_id) : max_tranc_id_(max_tranc_id) {
  // TODO: Lab2.2 实现 HeapIterator 构造函数
  for (const auto &item : item_vec) {
    items.push(item);
  }
  // 跳过不合法的值
  while (!top_value_legal()) {
    skip_by_tranc_id();  // 跳过不可见的事务
    while (!items.empty() && items.top().value_.empty()) {
      // 如果某个元素值被删除，则比它旧的相同key的元素应该跳过
      auto del_key = items.top().key_;
      while (!items.empty() && items.top().key_ == del_key) {
        items.pop();
      }
    }
  }
  update_current();  // 初始化 current
}

// ?语义奇怪
HeapIterator::pointer HeapIterator::operator->() const {
  // TODO: Lab2.2 实现 -> 重载
  update_current();
  return current.get();
}

HeapIterator::value_type HeapIterator::operator*() const {
  // TODO: Lab2.2 实现 * 重载

  return std::make_pair(items.top().key_, items.top().value_);
}

BaseIterator &HeapIterator::operator++() {
  // TODO: Lab2.2 实现 ++ 重载
  if (items.empty()) {
      return *this;  // 如果队列为空，直接返回
    }
    auto old_key = items.top().key_;
    items.pop();
    // 跳过重复的键
    while (!items.empty() && items.top().key_ == old_key) {
      items.pop();
    }
  // 跳过不合法的值
  while (!top_value_legal()) {
    skip_by_tranc_id();  // 跳过不可见的事务
    while (!items.empty() && items.top().value_.empty()) {
      // 如果某个元素值被删除，则比它旧的相同key的元素应该跳过
      auto del_key = items.top().key_;
      while (!items.empty() && items.top().key_ == del_key) {
        items.pop();
      }
    }
  }
  update_current();
  return *this;
}

bool HeapIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab2.2 实现 == 重载
  if (other.get_type() != IteratorType::HeapIterator) {
    return false;
  }
  auto other2 = dynamic_cast<const HeapIterator &>(other);
  if (items.empty() && other2.items.empty()) {
    return true;
  }
  if (items.empty() || other2.items.empty()) {
    return false;
  }
  return items.top().key_ == other2.items.top().key_ && items.top().value_ == other2.items.top().value_;
}

bool HeapIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab2.2 实现 != 重载
  return !(*this == other);
}

bool HeapIterator::top_value_legal() const {
  // TODO: Lab2.2 判断顶部元素是否合法
  // ? 被删除的值是不合法
  // ? 不允许访问的事务创建或更改的键值对不合法(暂时忽略)

  // 队列为空也是一种合法状态
  if (items.empty()) {
    return true;
  }

  // 如果没有开启事务功能, 则只需要检查值是否为空
  if (max_tranc_id_ == 0) {
    return !items.top().value_.empty();
  }

  // TODO:  实现事务相关的合法性检查
  return true;
}

void HeapIterator::skip_by_tranc_id() {
  // TODO: Lab2.2 后续的Lab实现, 只是作为标记提醒
  if (max_tranc_id_ == 0) {
    return;  // 如果没有开启事务功能, 则不需要跳过
  }
}

bool HeapIterator::is_end() const { return items.empty(); }
bool HeapIterator::is_valid() const { return !items.empty(); }

void HeapIterator::update_current() const {
  // current 缓存了当前键值对的值, 你实现 -> 重载时可能需要
  // TODO: Lab2.2 更新当前缓存值
  if (!items.empty()) {
    current = std::make_shared<value_type>(items.top().key_, items.top().value_);
  } else {
    current.reset();  // 清空当前缓存
  }
}

IteratorType HeapIterator::get_type() const { return IteratorType::HeapIterator; }

uint64_t HeapIterator::get_tranc_id() const { return max_tranc_id_; }
}  // namespace tiny_lsm