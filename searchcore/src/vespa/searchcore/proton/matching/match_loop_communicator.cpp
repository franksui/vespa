// Copyright Vespa.ai. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "match_loop_communicator.h"
#include <vespa/searchlib/features/first_phase_rank_lookup.h>
#include <vespa/vespalib/util/priority_queue.h>

using search::features::FirstPhaseRankLookup;

namespace proton:: matching {

MatchLoopCommunicator::MatchLoopCommunicator(size_t threads, size_t topN)
    : MatchLoopCommunicator(threads, topN, {}, nullptr)
{}
MatchLoopCommunicator::MatchLoopCommunicator(size_t threads, size_t topN, std::unique_ptr<IDiversifier> diversifier, FirstPhaseRankLookup* first_phase_rank_lookup)
    : _best_scores(),
      _best_dropped(),
      _estimate_match_frequency(threads),
      _get_second_phase_work(threads, topN, _best_scores, _best_dropped, std::move(diversifier), first_phase_rank_lookup),
      _complete_second_phase(threads, topN, _best_scores, _best_dropped)
{}
MatchLoopCommunicator::~MatchLoopCommunicator() = default;

void
MatchLoopCommunicator::EstimateMatchFrequency::mingle()
{
    double freqSum = 0.0;
    for (size_t i = 0; i < size(); ++i) {
        if (in(i).docs > 0) {
            double h = in(i).hits;
            double d = in(i).docs;
            freqSum += h/d;
        }
    }
    double freq = freqSum / size();
    for (size_t i = 0; i < size(); ++i) {
        out(i) = freq;
    }
}

namespace {

class NoRegisterFirstPhaseRank {
public:
    static void pick(uint32_t) noexcept { };
    static void drop() noexcept { }
};

class RegisterFirstPhaseRank {
    FirstPhaseRankLookup& _first_phase_rank_lookup;
    uint32_t _rank;
public:
    RegisterFirstPhaseRank(FirstPhaseRankLookup& first_phase_rank_lookup)
        : _first_phase_rank_lookup(first_phase_rank_lookup),
          _rank(0)
    {
    }
    void pick(uint32_t docid) noexcept { _first_phase_rank_lookup.add(docid, ++_rank); }
    void drop() noexcept { ++_rank; }
};

}

MatchLoopCommunicator::GetSecondPhaseWork::GetSecondPhaseWork(size_t n, size_t topN_in, Range &best_scores_in, BestDropped &best_dropped_in, std::unique_ptr<IDiversifier> diversifier, FirstPhaseRankLookup* first_phase_rank_lookup)
    : vespalib::Rendezvous<SortedHitSequence, TaggedHits, true>(n),
      topN(topN_in),
      best_scores(best_scores_in),
      best_dropped(best_dropped_in),
      _diversifier(std::move(diversifier)),
      _first_phase_rank_lookup(first_phase_rank_lookup)
{}

MatchLoopCommunicator::GetSecondPhaseWork::~GetSecondPhaseWork() = default;

// (SUI): 在这里去把 firstPhase 的 topK(rerankCount) 拿出来了
template<typename Q, typename F, typename R>
void
MatchLoopCommunicator::GetSecondPhaseWork::mingle(Q &queue, F &&accept, R register_first_phase_rank)
{
    size_t picked = 0;
    search::feature_t last_score = 0.0;
    while ((picked < topN) && !queue.empty()) {
        uint32_t i = queue.front();  // (SUI): 优先队列中排最前面的输入
        const Hit & hit = in(i).get(); // (SUI): 当前的 Hit
        if (accept(hit.first)) {
            register_first_phase_rank.pick(hit.first);  // (SUI): RegisterFirstPhaseRank 记录 firstPhase 排序的位置
            out(picked % size()).emplace_back(hit, i);  // (SUI): hit 以及所属的输入 In, 这个 i 后面是为了找到对应的 Out 输出
            last_score = hit.second;
            if (++picked == 1) {
                best_scores.high = hit.second;
            }
        } else {
            if (!best_dropped.valid) {
                best_dropped.valid = true;
                best_dropped.score = hit.second;
            }
            register_first_phase_rank.drop();
        }
        in(i).next(); // (SUI): 下一个
        if (in(i).valid()) {
            queue.adjust();
        } else {
            queue.pop_front();
        }
    }
    if (picked > 0) {
        best_scores.low = last_score;
    }
}

template<typename Q, typename R>
void
MatchLoopCommunicator::GetSecondPhaseWork::mingle(Q &queue, R register_first_phase_rank)
{
    if (_diversifier) {
        mingle(queue, [diversifier=_diversifier.get()](uint32_t docId) { return diversifier->accepted(docId);}, register_first_phase_rank);
    } else {
        mingle(queue, [](uint32_t) { return true;}, register_first_phase_rank);
    }
}

void
MatchLoopCommunicator::GetSecondPhaseWork::mingle()
{
    best_scores = Range();
    best_dropped.valid = false;
    size_t est_out = (topN / size()) + 1;
    vespalib::PriorityQueue<uint32_t, SelectCmp> queue(SelectCmp(*this));
    for (size_t i = 0; i < size(); ++i) {
        out(i).reserve(est_out);
        if (in(i).valid()) {  // (SUI): 这个时候 in 和 out 都已经被赋值了
            queue.push(i);
        }
    }
    if (_first_phase_rank_lookup != nullptr) {
        mingle(queue, RegisterFirstPhaseRank(*_first_phase_rank_lookup));
    } else {
        mingle(queue, NoRegisterFirstPhaseRank());
    }
}

void
MatchLoopCommunicator::CompleteSecondPhase::mingle()
{
    RangePair score_ranges(best_scores, Range());
    Range &new_scores = score_ranges.second;
    size_t est_out = (topN / size()) + 16;
    for (size_t i = 0; i < size(); ++i) {
        out(i).first.reserve(est_out);
    }
    for (size_t i = 0; i < size(); ++i) {
        for (const auto &[hit, tag]: in(i)) {
            out(tag).first.push_back(hit);
            new_scores.update(hit.second);
        }
    }
    if (score_ranges.first.isValid() && score_ranges.second.isValid()) {
        if (best_dropped.valid) {
            score_ranges.first.low = std::max(score_ranges.first.low, best_dropped.score);
        }
        for (size_t i = 0; i < size(); ++i) {
            out(i).second = score_ranges;
        }
    }
}

}
