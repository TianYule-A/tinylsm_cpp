#include "../../include/skiplist/skiplist.h"
#include <spdlog/spdlog.h>
#include <cstdint>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

namespace tiny_lsm {

// ************************ SkipListIterator ************************
BaseIterator &SkipListIterator::operator++() {
  // (done)TODO: Lab1.2 任务：实现SkipListIterator的++操作符
  current = current->forward_[0];
  return *this;
}

bool SkipListIterator::operator==(const BaseIterator &other) const {
  // (done)TODO: Lab1.2 任务：实现SkipListIterator的==操作符
  return current == dynamic_cast<const SkipListIterator &>(other).current;
}

bool SkipListIterator::operator!=(const BaseIterator &other) const {
  // (done)TODO: Lab1.2 任务：实现SkipListIterator的!=操作符
  return current != dynamic_cast<const SkipListIterator &>(other).current;
}

SkipListIterator::value_type SkipListIterator::operator*() const {
  // (done)TODO: Lab1.2 任务：实现SkipListIterator的*操作符
  return {get_key(), get_value()};
}

IteratorType SkipListIterator::get_type() const {
  // (done)TODO: Lab1.2 任务：实现SkipListIterator的get_type
  // ? 主要是为了熟悉基类的定义和继承关系
  return IteratorType::SkipListIterator;
}

bool SkipListIterator::is_valid() const { return current && !current->key_.empty(); }
bool SkipListIterator::is_end() const { return current == nullptr; }

std::string SkipListIterator::get_key() const { return current->key_; }
std::string SkipListIterator::get_value() const { return current->value_; }
uint64_t SkipListIterator::get_tranc_id() const { return current->tranc_id_; }

// ************************ SkipList ************************
// 构造函数
SkipList::SkipList(int max_lvl) : max_level(max_lvl), current_level(1) {
  head = std::make_shared<SkipListNode>("", "", max_level, 0);
  dis_01 = std::uniform_int_distribution<>(0, 1);
  dis_level = std::uniform_int_distribution<>(0, (1 << max_lvl) - 1);
  gen = std::mt19937(std::random_device()());
}

int SkipList::random_level() {
  // ? 通过"抛硬币"的方式随机生成层数：
  // ? - 每次有50%的概率增加一层
  // ? - 确保层数分布为：第1层100%，第2层50%，第3层25%，以此类推
  // ? - 层数范围限制在[1, max_level]之间，避免浪费内存
  // √TODO√: Lab1.1 任务：插入时随机为这一次操作确定其最高连接的链表层数
  // 生成随机数
  int level = 1;
  while (level < max_level && dis_01(gen) == 1) {
    level++;
  }
  return level;
}

// 插入或更新键值对
void SkipList::put(const std::string &key, const std::string &value, uint64_t tranc_id) {
  spdlog::trace("SkipList--put({}, {}, {})", key, value, tranc_id);

  // (done)TODO: Lab1.1  任务：实现插入或更新键值对
  // ? Hint: 你需要保证不同`Level`的步长从底层到高层逐渐增加
  // ? 你可能需要使用到`random_level`函数以确定层数, 其注释中为你提供一种思路
  // ? tranc_id 为事务id, 现在你不需要关注它, 直接将其传递到 SkipListNode的构造函数中即可
  int new_level = random_level();
  std::vector<std::shared_ptr<SkipListNode>> update_forward(max_level, nullptr);
  // 创建新节点
  auto new_node = std::make_shared<SkipListNode>(key, value, new_level, tranc_id);
  auto current = head;
  // 寻找插入位置
  for (int level = current_level - 1; level >= 0; --level) {
    while (current->forward_[level] && *current->forward_[level] < *new_node) {
      current = current->forward_[level];
    }
    update_forward[level] = current;
  }
  current = current->forward_[0];
  // key已存在, 更新值
  if (current && current->key_ == key) {
    size_bytes += value.size() - current->value_.size();
    current->value_ = value;
    return;
  }
  // key不存在, 插入新节点,更新各层指针
  // 如果新节点的层数大于当前跳表的层数，更新更高层的指针head
  if (new_level > current_level) {
    for (int level = current_level; level < new_level; ++level) {
      update_forward[level] = head;
    }
    current_level = new_level;
  }

  for (int level = 0; level < new_level; ++level) {
    new_node->forward_[level] = update_forward[level]->forward_[level];
    update_forward[level]->forward_[level] = new_node;
    new_node->set_backward(level, update_forward[level]);
    // new_node下一节点可能为空
    if (new_node->forward_[level]) {
      new_node->forward_[level]->set_backward(level, new_node);
    }
  }

  size_bytes += sizeof(uint64_t) + key.size() + value.size();
}

// 查找键值对
SkipListIterator SkipList::get(const std::string &key, uint64_t tranc_id) {
  spdlog::trace("SkipList--get({}) called", key);
  // ? 你可以参照上面的注释完成日志输出以便于调试
  // ? 日志为输出到你执行二进制所在目录下的log文件夹

  // (done)TODO: Lab1.1 任务：实现查找键值对,
  // (done)TODO: 并且你后续需要额外实现SkipListIterator中的TODO部分(Lab1.2)
  auto current = head;
  for (int level = current_level - 1; level >= 0; --level) {
    while (current->forward_[level] && current->forward_[level]->key_ < key) {
      current = current->forward_[level];
    }
  }
  current = current->forward_[0];
  if (current && current->key_ == key) {
    // 找到节点
    return SkipListIterator(current);
  }
  // 没找到返回空
  spdlog::trace("SkipList--get({}) not found", key);
  return SkipListIterator{};
}

// 删除键值对
// ! 这里的 remove 是跳表本身真实的 remove,  lsm 应该使用 put 空值表示删除,
// ! 这里只是为了实现完整的 SkipList 不会真正被上层调用
void SkipList::remove(const std::string &key) {
  // (done)TODO: Lab1.1 任务：实现删除键值对
  auto current = head;
  std::vector<std::shared_ptr<SkipListNode>> update(max_level, nullptr);
  for (int level = current_level - 1; level >= 0; --level) {
    while (current->forward_[level] && current->forward_[level]->key_ < key) {
      current = current->forward_[level];
    }
    update[level] = current;
  }
  current = current->forward_[0];
  if (!current || current->key_ != key) {
    // 没找到节点
    spdlog::trace("SkipList--remove({}) not found", key);
    return;
  }
  // 找到要删除的节点current
  for (int level = 0; level < current_level; ++level) {
    if (update[level]->forward_[level] != current) {
      break;  // 如果前驱节点的 forward 指针不指向当前节点，说明更高层没有要删除的节点
    }
    // 更新前驱节点的 forward 指针
    update[level]->forward_[level] = current->forward_[level];
  }
  // 更新内存大小
  size_bytes -= sizeof(uint64_t) + key.length() + current->value_.length();
  // 更新后继节点的 backward 指针
  for (int level = 0; level < current->backward_.size(); ++level) {
    if (current->forward_[level]) {
      current->forward_[level]->set_backward(level, update[level]);
    }
  }
  // 如果当前节点是最高层的节点，更新当前层级
  if (current_level > 1 && head->forward_[current_level - 1] == nullptr) {
    current_level--;
  }
}

// 刷盘时可以直接遍历最底层链表
std::vector<std::tuple<std::string, std::string, uint64_t>> SkipList::flush() {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);
  spdlog::debug("SkipList--flush(): Starting to flush skiplist data");

  std::vector<std::tuple<std::string, std::string, uint64_t>> data;
  auto node = head->forward_[0];
  while (node) {
    data.emplace_back(node->key_, node->value_, node->tranc_id_);
    node = node->forward_[0];
  }

  spdlog::debug("SkipList--flush(): Flushed {} entries", data.size());

  return data;
}

size_t SkipList::get_size() {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);
  return size_bytes;
}

// 清空跳表，释放内存
void SkipList::clear() {
  // std::unique_lock<std::shared_mutex> lock(rw_mutex);
  head = std::make_shared<SkipListNode>("", "", max_level, 0);
  size_bytes = 0;
}

SkipListIterator SkipList::begin() {
  // return SkipListIterator(head->forward[0], rw_mutex);
  return SkipListIterator(head->forward_[0]);
}

SkipListIterator SkipList::end() {
  return SkipListIterator();  // 使用空构造函数
}

// 找到前缀的起始位置
// 返回第一个前缀匹配或者大于前缀的迭代器
SkipListIterator SkipList::begin_preffix(const std::string &preffix) {
  // TODO: Lab1.3 任务：实现前缀查询的起始位置
  auto current = head;
  auto preffix_length = preffix.length();
  for (int level = current_level - 1; level >= 0; --level) {
    while (current->forward_[level] && current->forward_[level]->key_.compare(0, preffix_length, preffix) < 0) {
      current = current->forward_[level];
    }
  }
  if (current->forward_[0] && current->forward_[0]->key_.compare(0, preffix_length, preffix) == 0) {
    // 找到前缀匹配的节点
    return SkipListIterator(current->forward_[0]);
  }
  return SkipListIterator{};
}

// 找到前缀的终结位置
SkipListIterator SkipList::end_preffix(const std::string &preffix) {
  // TODO: Lab1.3 任务：实现前缀查询的终结位置
  auto current = head;
  auto preffix_length = preffix.length();
  // 寻找第一个大于前缀的节点
  for (int level = current_level - 1; level >= 0; --level) {
    while (current->forward_[level] && current->forward_[level]->key_.compare(0, preffix_length, preffix) <= 0) {
      current = current->forward_[level];
    }
  }
  if (current->forward_[0] && current->forward_[0]->key_.compare(0, preffix_length, preffix) > 0) {
    return SkipListIterator(current->forward_[0]);
  }

  return SkipListIterator();  // 返回一个空迭代器
}

// ? 这里单调谓词的含义是, 整个数据库只会有一段连续区间满足此谓词
// ? 例如之前特化的前缀查询，以及后续可能的范围查询，都可以转化为谓词查询
// ? 返回第一个满足谓词的位置和最后一个满足谓词的迭代器
// ? 如果不存在, 范围nullptr
// ? 谓词作用于key, 且保证满足谓词的结果只在一段连续的区间内, 例如前缀匹配的谓词
// ? predicate返回值:
// ?   0: 满足谓词
// ?   >0: 不满足谓词, 需要向右移动
// ?   <0: 不满足谓词, 需要向左移动
// ! Skiplist 中的谓词查询不会进行事务id的判断, 需要上层自己进行判断
std::optional<std::pair<SkipListIterator, SkipListIterator>> SkipList::iters_monotony_predicate(
    std::function<int(const std::string &)> predicate) {
  // (done)TODO: Lab1.3 任务：实现谓词查询的起始位置
  auto current = head;
  // 迭代器起始与结束节点
  std::shared_ptr<SkipListNode> start_node, end_node = nullptr;
  // 寻找一个满足谓词的节点
  bool found_flag = false;
  int level = current_level - 1;
  for (; level >= 0; --level) {
    while (current->forward_[level] && predicate(current->forward_[level]->key_) > 0) {
      // 向右移动
      current = current->forward_[level];
    }
    if (current->forward_[level] && predicate(current->forward_[level]->key_) == 0) {
      // 找到满足谓词的节点
      current = current->forward_[level];
      found_flag = true;
      break;
    }
  }
  if (found_flag) {
    int start_level = level, end_level = level;
    std::weak_ptr<SkipListNode> start_node_weak = current;
    end_node = current;
    // 向右遍历直到不满足谓词
    for (int i = level; i >= 0; --i) {
      while (end_node->forward_[i] && predicate(end_node->forward_[i]->key_) == 0) {
        end_node = end_node->forward_[i];
      }
    }
    // 向左遍历直到不满足谓词
    for (int i = level; i >= 0; --i) {
      while (start_node_weak.lock()->backward_[i].lock() &&
             predicate(start_node_weak.lock()->backward_[i].lock()->key_) == 0) {
        start_node_weak = start_node_weak.lock()->backward_[i].lock();
      }
    }
    start_node = start_node_weak.lock();
    end_node = end_node->forward_[0];
    return std::make_pair(SkipListIterator(start_node), SkipListIterator(end_node));
  }

  return std::nullopt;
}

// ? 打印跳表, 你可以在出错时调用此函数进行调试
void SkipList::print_skiplist() {
  for (int level = 0; level < current_level; level++) {
    std::cout << "Level " << level << ": ";
    auto current = head->forward_[level];
    while (current) {
      std::cout << current->key_ << "(" << current->value_ << ")";
      current = current->forward_[level];
      if (current) {
        std::cout << " -> ";
      }
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;
}
}  // namespace tiny_lsm