// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include <vespa/vespalib/stllike/hash_set.h>

namespace search::tensor {

class HnswIndex;

class HashSetVisitedTracker
{
    vespalib::hash_set<uint32_t> _visited;
public:
    HashSetVisitedTracker(const HnswIndex&, uint32_t, uint32_t estimated_visisted_nodes);
    ~HashSetVisitedTracker();
    void mark(uint32_t doc_id) { _visited.insert(doc_id); }
    bool try_mark(uint32_t doc_id) {
        return _visited.insert(doc_id).second;
    }
};

}
