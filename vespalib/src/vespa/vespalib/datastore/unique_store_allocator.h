// Copyright 2019 Oath Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "datastore.h"
#include "entryref.h"
#include "unique_store_add_result.h"
#include "unique_store_entry.h"
#include "i_compactable.h"

namespace search::datastore {

/**
 * Allocator for unique values of type EntryT that is accessed via a
 * 32-bit EntryRef.
 */
template <typename EntryT, typename RefT = EntryRefT<22> >
class UniqueStoreAllocator : public ICompactable
{
public:
    using DataStoreType = DataStoreT<RefT>;
    using EntryType = EntryT;
    using WrappedEntryType = UniqueStoreEntry<EntryType>;
    using RefType = RefT;
    using UniqueStoreBufferType = BufferType<WrappedEntryType>;
private:
    DataStoreType _store;
    UniqueStoreBufferType _typeHandler;

public:
    UniqueStoreAllocator();
    ~UniqueStoreAllocator() override;
    EntryRef allocate(const EntryType& value);
    void hold(EntryRef ref);
    EntryRef move(EntryRef ref) override;
    const WrappedEntryType& get_wrapped(EntryRef ref) const {
        RefType iRef(ref);
        return *_store.template getEntry<WrappedEntryType>(iRef);
    }
    const EntryType& get(EntryRef ref) const {
        return get_wrapped(ref).value();
    }
    DataStoreType& get_data_store() { return _store; }
};

}
