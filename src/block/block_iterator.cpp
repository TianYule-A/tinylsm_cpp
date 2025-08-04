#include "../../include/block/block_iterator.h"
#include "../../include/block/block.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>

class Block;

namespace tiny_lsm {
BlockIterator::BlockIterator(std::shared_ptr<Block> b, size_t index,
                             uint64_t tranc_id)
    : block(b), current_index(index), tranc_id_(tranc_id),
      cached_value(std::nullopt) {
  skip_by_tranc_id();
}

BlockIterator::BlockIterator(std::shared_ptr<Block> b, const std::string &key,
                             uint64_t tranc_id)
    : block(b), tranc_id_(tranc_id), cached_value(std::nullopt) {
  // TODO: Lab3.2 创建迭代器时直接移动到指定的key位置
  // ? 你需要借助之前实现的 Block 类的成员函数
  auto idx = block->get_idx_binary(key, tranc_id_);
  if (idx.has_value()) {
    current_index = *idx;
  }else {
    // 如果没有找到对应的key, 则设置为end迭代器
    current_index = block->offsets.size();
  }
}

// BlockIterator::BlockIterator(std::shared_ptr<Block> b, uint64_t tranc_id)
//     : block(b), current_index(0), tranc_id_(tranc_id),
//       cached_value(std::nullopt) {
//   skip_by_tranc_id();
// }

BlockIterator::pointer BlockIterator::operator->() const {
  // TODO: Lab3.2 -> 重载
  update_current();
return &(*cached_value);
    
}

BlockIterator &BlockIterator::operator++() {
  // TODO: Lab3.2 ++ 重载
  // ? 在后续的Lab实现事务后，你可能需要对这个函数进行返修
  if (block && current_index < block->offsets.size()) {
    size_t pre_idx = current_index;
    uint16_t pre_offset = block->get_offset_at(pre_idx);
    auto entry = block->get_entry_at(pre_offset);
    current_index++;
    // 跳过重复的key
    while (current_index < block->offsets.size() && block->is_same_key(current_index, entry.key)) {
      pre_idx = current_index;
      pre_offset = block->get_offset_at(pre_idx);
      entry = block->get_entry_at(pre_offset);

      current_index++;
    }
    skip_by_tranc_id();
}
return *this;
}

bool BlockIterator::operator==(const BlockIterator &other) const {
  // TODO: Lab3.2 == 重载
  return block == other.block &&
         current_index == other.current_index;
}

bool BlockIterator::operator!=(const BlockIterator &other) const {
  // TODO: Lab3.2 != 重载
  return !(*this == other);
}

BlockIterator::value_type BlockIterator::operator*() const {
  // TODO: Lab3.2 * 重载
//   if (block == nullptr || current_index >= block->offsets.size()) {
//     throw std::out_of_range("BlockIterator out of range");
//   }
  update_current();
  return *cached_value; // 返回缓存的值
}

bool BlockIterator::is_end() { return current_index == block->offsets.size(); }

void BlockIterator::update_current() const {
  // TODO: Lab3.2 更新当前指针
  // ? 该函数是可选的实现, 你可以采用自己的其他方案实现->, 而不是使用
  // ? cached_value 来缓存当前指针
  if (block == nullptr || current_index >= block->offsets.size()) {
    throw std::out_of_range("BlockIterator out of range");
  }
  auto offset = block->get_offset_at(current_index);
  auto key = block->get_key_at(offset);
  auto value = block->get_value_at(offset);
  cached_value = std::make_pair(key, value);
}

void BlockIterator::skip_by_tranc_id() {
  // TODO: Lab3.2 * 跳过事务ID
  // ? 只是进行标记以供你在后续Lab实现事务功能后修改
  // ? 现在你不需要考虑这个函数
}
} // namespace tiny_lsm