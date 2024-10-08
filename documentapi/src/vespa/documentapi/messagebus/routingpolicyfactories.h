// Copyright Vespa.ai. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include "iroutingpolicyfactory.h"

namespace document { class IDocumentTypeRepo; }

namespace documentapi {

class RoutingPolicyFactories {
private:
    RoutingPolicyFactories() { /* abstract */ }

public:
    class AndPolicyFactory : public IRoutingPolicyFactory {
    public:
        mbus::IRoutingPolicy::UP createPolicy(const string &param) const override;
    };
    class MessageTypePolicyFactory : public IRoutingPolicyFactory {
    public:
        mbus::IRoutingPolicy::UP createPolicy(const string &param) const override;
    };
    class ContentPolicyFactory : public IRoutingPolicyFactory {
    public:
        mbus::IRoutingPolicy::UP createPolicy(const string &param) const override;
    };
    class LoadBalancerPolicyFactory : public IRoutingPolicyFactory {
    public:
        mbus::IRoutingPolicy::UP createPolicy(const string &param) const override;
    };
    class DocumentRouteSelectorPolicyFactory : public IRoutingPolicyFactory {
    private:
        const document::IDocumentTypeRepo &_repo;
        string _configId;
    public:
        DocumentRouteSelectorPolicyFactory(const document::IDocumentTypeRepo &repo, const string &configId);
        mbus::IRoutingPolicy::UP createPolicy(const string &param) const override;
    };
    class ExternPolicyFactory : public IRoutingPolicyFactory {
    public:
        mbus::IRoutingPolicy::UP createPolicy(const string &param) const override;
    };
    class LocalServicePolicyFactory : public IRoutingPolicyFactory {
    public:
        mbus::IRoutingPolicy::UP createPolicy(const string &param) const override;
    };
    class RoundRobinPolicyFactory : public IRoutingPolicyFactory {
    public:
        mbus::IRoutingPolicy::UP createPolicy(const string &param) const override;
    };
    class SubsetServicePolicyFactory : public IRoutingPolicyFactory {
    public:
        mbus::IRoutingPolicy::UP createPolicy(const string &param) const override;
    };
};

}
