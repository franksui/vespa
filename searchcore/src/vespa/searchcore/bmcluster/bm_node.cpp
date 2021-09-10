// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "bm_node.h"
#include "bm_cluster.h"
#include "bm_cluster_params.h"
#include "bm_message_bus.h"
#include "bm_storage_chain_builder.h"
#include "bm_storage_link_context.h"
#include "storage_api_chain_bm_feed_handler.h"
#include "storage_api_message_bus_bm_feed_handler.h"
#include "storage_api_rpc_bm_feed_handler.h"
#include "document_api_message_bus_bm_feed_handler.h"
#include "i_bm_feed_handler.h"
#include "spi_bm_feed_handler.h"
#include <vespa/config-attributes.h>
#include <vespa/config-bucketspaces.h>
#include <vespa/config-imported-fields.h>
#include <vespa/config-indexschema.h>
#include <vespa/config-persistence.h>
#include <vespa/config-rank-profiles.h>
#include <vespa/config-slobroks.h>
#include <vespa/config-stor-distribution.h>
#include <vespa/config-stor-filestor.h>
#include <vespa/config-summary.h>
#include <vespa/config-summarymap.h>
#include <vespa/config-upgrading.h>
#include <vespa/config/common/configcontext.h>
#include <vespa/document/bucket/bucketspace.h>
#include <vespa/document/fieldset/fieldsetrepo.h>
#include <vespa/document/repo/configbuilder.h>
#include <vespa/document/repo/document_type_repo_factory.h>
#include <vespa/document/repo/documenttyperepo.h>
#include <vespa/document/test/make_bucket_space.h>
#include <vespa/messagebus/config-messagebus.h>
#include <vespa/messagebus/testlib/slobrok.h>
#include <vespa/metrics/config-metricsmanager.h>
#include <vespa/searchcommon/common/schemaconfigurer.h>
#include <vespa/searchcore/proton/common/alloc_config.h>
#include <vespa/searchcore/proton/matching/querylimiter.h>
#include <vespa/searchcore/proton/metrics/metricswireservice.h>
#include <vespa/searchcore/proton/persistenceengine/ipersistenceengineowner.h>
#include <vespa/searchcore/proton/persistenceengine/i_resource_write_filter.h>
#include <vespa/searchcore/proton/persistenceengine/persistenceengine.h>
#include <vespa/searchcore/proton/server/bootstrapconfig.h>
#include <vespa/searchcore/proton/server/documentdb.h>
#include <vespa/searchcore/proton/server/document_db_maintenance_config.h>
#include <vespa/searchcore/proton/server/documentdbconfigmanager.h>
#include <vespa/searchcore/proton/server/fileconfigmanager.h>
#include <vespa/searchcore/proton/server/memoryconfigstore.h>
#include <vespa/searchcore/proton/server/persistencehandlerproxy.h>
#include <vespa/searchcore/proton/test/disk_mem_usage_notifier.h>
#include <vespa/searchlib/index/dummyfileheadercontext.h>
#include <vespa/searchlib/transactionlog/translogserver.h>
#include <vespa/searchsummary/config/config-juniperrc.h>
#include <vespa/storage/bucketdb/config-stor-bucket-init.h>
#include <vespa/storage/common/i_storage_chain_builder.h>
#include <vespa/storage/config/config-stor-bouncer.h>
#include <vespa/storage/config/config-stor-communicationmanager.h>
#include <vespa/storage/config/config-stor-distributormanager.h>
#include <vespa/storage/config/config-stor-opslogger.h>
#include <vespa/storage/config/config-stor-prioritymapping.h>
#include <vespa/storage/config/config-stor-server.h>
#include <vespa/storage/config/config-stor-status.h>
#include <vespa/storage/config/config-stor-visitordispatcher.h>
#include <vespa/storage/storageserver/rpc/shared_rpc_resources.h>
#include <vespa/storage/visiting/config-stor-visitor.h>
#include <vespa/storageserver/app/distributorprocess.h>
#include <vespa/storageserver/app/servicelayerprocess.h>
#include <vespa/vespalib/io/fileutil.h>
#include <vespa/vespalib/util/size_literals.h>
#include <tests/proton/common/dummydbowner.h>

#include <vespa/log/log.h>
LOG_SETUP(".bmcluster.bm_node");

using cloud::config::SlobroksConfigBuilder;
using cloud::config::filedistribution::FiledistributorrpcConfig;
using config::ConfigSet;
using document::BucketSpace;
using document::DocumenttypesConfig;
using document::DocumenttypesConfigBuilder;
using document::DocumentType;
using document::DocumentTypeRepo;
using document::Field;
using messagebus::MessagebusConfigBuilder;
using metrics::MetricsmanagerConfigBuilder;
using proton::BootstrapConfig;
using proton::DocTypeName;
using proton::DocumentDB;
using proton::DocumentDBConfig;
using proton::HwInfo;
using search::index::Schema;
using search::index::SchemaBuilder;
using search::transactionlog::TransLogServer;
using storage::rpc::SharedRpcResources;
using storage::rpc::StorageApiRpcService;
using storage::spi::PersistenceProvider;
using vespa::config::content::PersistenceConfigBuilder;
using vespa::config::content::StorDistributionConfigBuilder;
using vespa::config::content::StorFilestorConfigBuilder;
using vespa::config::content::UpgradingConfigBuilder;
using vespa::config::content::core::BucketspacesConfig;
using vespa::config::content::core::BucketspacesConfigBuilder;
using vespa::config::content::core::StorBouncerConfigBuilder;
using vespa::config::content::core::StorBucketInitConfigBuilder;
using vespa::config::content::core::StorCommunicationmanagerConfigBuilder;
using vespa::config::content::core::StorDistributormanagerConfigBuilder;
using vespa::config::content::core::StorOpsloggerConfigBuilder;
using vespa::config::content::core::StorPrioritymappingConfigBuilder;
using vespa::config::content::core::StorServerConfigBuilder;
using vespa::config::content::core::StorStatusConfigBuilder;
using vespa::config::content::core::StorVisitorConfigBuilder;
using vespa::config::content::core::StorVisitordispatcherConfigBuilder;
using vespa::config::search::AttributesConfig;
using vespa::config::search::AttributesConfigBuilder;
using vespa::config::search::ImportedFieldsConfig;
using vespa::config::search::IndexschemaConfig;
using vespa::config::search::RankProfilesConfig;
using vespa::config::search::SummaryConfig;
using vespa::config::search::SummarymapConfig;
using vespa::config::search::core::ProtonConfig;
using vespa::config::search::core::ProtonConfigBuilder;
using vespa::config::search::summary::JuniperrcConfig;
using vespalib::compression::CompressionConfig;

namespace search::bmcluster {

vespalib::string base_dir = "testdb";

std::shared_ptr<AttributesConfig> make_attributes_config() {
    AttributesConfigBuilder builder;
    AttributesConfig::Attribute attribute;
    attribute.name = "int";
    attribute.datatype = AttributesConfig::Attribute::Datatype::INT32;
    builder.attribute.emplace_back(attribute);
    return std::make_shared<AttributesConfig>(builder);
}

std::shared_ptr<DocumentDBConfig> make_document_db_config(std::shared_ptr<DocumenttypesConfig> document_types, std::shared_ptr<const DocumentTypeRepo> repo, const DocTypeName& doc_type_name)
{
    auto indexschema = std::make_shared<IndexschemaConfig>();
    auto attributes = make_attributes_config();
    auto summary = std::make_shared<SummaryConfig>();
    std::shared_ptr<Schema> schema(new Schema());
    SchemaBuilder::build(*indexschema, *schema);
    SchemaBuilder::build(*attributes, *schema);
    SchemaBuilder::build(*summary, *schema);
    return std::make_shared<DocumentDBConfig>(
            1,
            std::make_shared<RankProfilesConfig>(),
            std::make_shared<proton::matching::RankingConstants>(),
            std::make_shared<proton::matching::RankingExpressions>(),
            std::make_shared<proton::matching::OnnxModels>(),
            indexschema,
            attributes,
            summary,
            std::make_shared<SummarymapConfig>(),
            std::make_shared<JuniperrcConfig>(),
            document_types,
            repo,
            std::make_shared<ImportedFieldsConfig>(),
            std::make_shared<TuneFileDocumentDB>(),
            schema,
            std::make_shared<proton::DocumentDBMaintenanceConfig>(),
            search::LogDocumentStore::Config(),
            std::make_shared<const proton::ThreadingServiceConfig>(proton::ThreadingServiceConfig::make(1)),
            std::make_shared<const proton::AllocConfig>(),
            "client",
            doc_type_name.getName());
}

void
make_slobroks_config(SlobroksConfigBuilder& slobroks, int slobrok_port)
{
    SlobroksConfigBuilder::Slobrok slobrok;
    slobrok.connectionspec = vespalib::make_string("tcp/localhost:%d", slobrok_port);
    slobroks.slobrok.push_back(std::move(slobrok));
}

void
make_bucketspaces_config(BucketspacesConfigBuilder& bucketspaces)
{
    BucketspacesConfigBuilder::Documenttype bucket_space_map;
    bucket_space_map.name = "test";
    bucket_space_map.bucketspace = "default";
    bucketspaces.documenttype.emplace_back(std::move(bucket_space_map));
}

class MyPersistenceEngineOwner : public proton::IPersistenceEngineOwner
{
    void setClusterState(BucketSpace, const storage::spi::ClusterState&) override { }
};

struct MyResourceWriteFilter : public proton::IResourceWriteFilter
{
    bool acceptWriteOperation() const override { return true; }
    State getAcceptState() const override { return IResourceWriteFilter::State(); }
};

class MyServiceLayerProcess : public storage::ServiceLayerProcess {
    PersistenceProvider&    _provider;

public:
    MyServiceLayerProcess(const config::ConfigUri&  configUri,
                          PersistenceProvider& provider,
                          std::unique_ptr<storage::IStorageChainBuilder> chain_builder);
    ~MyServiceLayerProcess() override { shutdown(); }

    void shutdown() override;
    void setupProvider() override;
    PersistenceProvider& getProvider() override;
};

MyServiceLayerProcess::MyServiceLayerProcess(const config::ConfigUri&  configUri,
                                             PersistenceProvider& provider,
                                             std::unique_ptr<storage::IStorageChainBuilder> chain_builder)
    : ServiceLayerProcess(configUri),
      _provider(provider)
{
    if (chain_builder) {
        set_storage_chain_builder(std::move(chain_builder));
    }
}

void
MyServiceLayerProcess::shutdown()
{
    ServiceLayerProcess::shutdown();
}

void
MyServiceLayerProcess::setupProvider()
{
}

PersistenceProvider&
MyServiceLayerProcess::getProvider()
{
    return _provider;
}

struct StorageConfigSet
{
    vespalib::string              config_id;
    DocumenttypesConfigBuilder    documenttypes;
    StorDistributionConfigBuilder stor_distribution;
    StorBouncerConfigBuilder      stor_bouncer;
    StorCommunicationmanagerConfigBuilder stor_communicationmanager;
    StorOpsloggerConfigBuilder    stor_opslogger;
    StorPrioritymappingConfigBuilder stor_prioritymapping;
    UpgradingConfigBuilder        upgrading;
    StorServerConfigBuilder       stor_server;
    StorStatusConfigBuilder       stor_status;
    BucketspacesConfigBuilder     bucketspaces;
    MetricsmanagerConfigBuilder   metricsmanager;
    SlobroksConfigBuilder         slobroks;
    MessagebusConfigBuilder       messagebus;

    StorageConfigSet(bool distributor, const vespalib::string& config_id_in, const DocumenttypesConfig& documenttypes_in,
                     int slobrok_port, int mbus_port, int rpc_port, int status_port, const BmClusterParams& params)
        : config_id(config_id_in),
          documenttypes(documenttypes_in),
          stor_distribution(),
          stor_bouncer(),
          stor_communicationmanager(),
          stor_opslogger(),
          stor_prioritymapping(),
          upgrading(),
          stor_server(),
          stor_status(),
          bucketspaces(),
          metricsmanager(),
          slobroks(),
          messagebus()
    {
        {
            auto& dc = stor_distribution;
            {
                StorDistributionConfigBuilder::Group group;
                {
                    StorDistributionConfigBuilder::Group::Nodes node;
                    node.index = 0;
                    group.nodes.push_back(std::move(node));
                }
                group.index = "invalid";
                group.name = "invalid";
                group.capacity = 1.0;
                group.partitions = "";
                dc.group.push_back(std::move(group));
            }
            dc.redundancy = 1;
            dc.readyCopies = 1;
        }
        stor_server.isDistributor = distributor;
        stor_server.contentNodeBucketDbStripeBits = params.get_bucket_db_stripe_bits();
        if (distributor) {
            stor_server.rootFolder = "distributor";
        } else {
            stor_server.rootFolder = "storage";
        }
        make_slobroks_config(slobroks, slobrok_port);
        stor_communicationmanager.rpc.numNetworkThreads = params.get_rpc_network_threads();
        stor_communicationmanager.rpc.eventsBeforeWakeup = params.get_rpc_events_before_wakeup();
        stor_communicationmanager.rpc.numTargetsPerNode = params.get_rpc_targets_per_node();
        stor_communicationmanager.mbusport = mbus_port;
        stor_communicationmanager.rpcport = rpc_port;
        stor_communicationmanager.skipThread = params.get_skip_communicationmanager_thread();

        stor_status.httpport = status_port;
        make_bucketspaces_config(bucketspaces);
    }

    ~StorageConfigSet();

    void add_builders(ConfigSet& set) {
        set.addBuilder(config_id, &documenttypes);
        set.addBuilder(config_id, &stor_distribution);
        set.addBuilder(config_id, &stor_bouncer);
        set.addBuilder(config_id, &stor_communicationmanager);
        set.addBuilder(config_id, &stor_opslogger);
        set.addBuilder(config_id, &stor_prioritymapping);
        set.addBuilder(config_id, &upgrading);
        set.addBuilder(config_id, &stor_server);
        set.addBuilder(config_id, &stor_status);
        set.addBuilder(config_id, &bucketspaces);
        set.addBuilder(config_id, &metricsmanager);
        set.addBuilder(config_id, &slobroks);
        set.addBuilder(config_id, &messagebus);
    }
};

StorageConfigSet::~StorageConfigSet() = default;

struct ServiceLayerConfigSet : public StorageConfigSet
{
    PersistenceConfigBuilder      persistence;
    StorFilestorConfigBuilder     stor_filestor;
    StorBucketInitConfigBuilder   stor_bucket_init;
    StorVisitorConfigBuilder      stor_visitor;

    ServiceLayerConfigSet(const vespalib::string& config_id_in, const DocumenttypesConfig& documenttypes_in,
                         int slobrok_port, int mbus_port, int rpc_port, int status_port, const BmClusterParams& params)
        : StorageConfigSet(false, config_id_in, documenttypes_in, slobrok_port, mbus_port, rpc_port, status_port, params),
          persistence(),
          stor_filestor(),
          stor_bucket_init(),
          stor_visitor()
    {
        stor_filestor.numResponseThreads = params.get_response_threads();
        stor_filestor.numNetworkThreads = params.get_rpc_network_threads();
        stor_filestor.useAsyncMessageHandlingOnSchedule = params.get_use_async_message_handling_on_schedule();
    }

    ~ServiceLayerConfigSet();

    void add_builders(ConfigSet& set) {
        StorageConfigSet::add_builders(set);
        set.addBuilder(config_id, &persistence);
        set.addBuilder(config_id, &stor_filestor);
        set.addBuilder(config_id, &stor_bucket_init);
        set.addBuilder(config_id, &stor_visitor);
    }
};

ServiceLayerConfigSet::~ServiceLayerConfigSet() = default;

struct DistributorConfigSet : public StorageConfigSet
{
    StorDistributormanagerConfigBuilder stor_distributormanager;
    StorVisitordispatcherConfigBuilder  stor_visitordispatcher;

    DistributorConfigSet(const vespalib::string& config_id_in, const DocumenttypesConfig& documenttypes_in,
                         int slobrok_port, int mbus_port, int rpc_port, int status_port, const BmClusterParams& params)
        : StorageConfigSet(true, config_id_in, documenttypes_in, slobrok_port, mbus_port, rpc_port, status_port, params),
          stor_distributormanager(),
          stor_visitordispatcher()
    {
        stor_distributormanager.numDistributorStripes = params.get_distributor_stripes();
    }

    ~DistributorConfigSet();

    void add_builders(ConfigSet& set) {
        StorageConfigSet::add_builders(set);
        set.addBuilder(config_id, &stor_distributormanager);
        set.addBuilder(config_id, &stor_visitordispatcher);
    }
};

DistributorConfigSet::~DistributorConfigSet() = default;

BmNode::BmNode(std::shared_ptr<document::DocumenttypesConfig> document_types)
    : _document_types(std::move(document_types)),
      _repo(document::DocumentTypeRepoFactory::make(*_document_types)),
      _doc_type_name("test"),
      _document_type(_repo->getDocumentType(_doc_type_name.getName())),
      _field(_document_type->getField("int"))
{
}

BmNode::~BmNode() = default;

class MyBmNode : public BmNode
{
    std::shared_ptr<DocumentDBConfig>          _document_db_config;
    vespalib::string                           _base_dir;
    search::index::DummyFileHeaderContext      _file_header_context;
    int                                        _tls_listen_port;
    int                                        _slobrok_port;
    int                                        _service_layer_mbus_port;
    int                                        _service_layer_rpc_port;
    int                                        _service_layer_status_port;
    int                                        _distributor_mbus_port;
    int                                        _distributor_rpc_port;
    int                                        _distributor_status_port;
    TransLogServer                             _tls;
    vespalib::string                           _tls_spec;
    proton::matching::QueryLimiter             _query_limiter;
    vespalib::Clock                            _clock;
    proton::DummyWireService                   _metrics_wire_service;
    proton::MemoryConfigStores                 _config_stores;
    vespalib::ThreadStackExecutor              _summary_executor;
    proton::DummyDBOwner                       _document_db_owner;
    BucketSpace                                _bucket_space;
    std::shared_ptr<DocumentDB>                _document_db;
    MyPersistenceEngineOwner                   _persistence_owner;
    MyResourceWriteFilter                      _write_filter;
    proton::test::DiskMemUsageNotifier         _disk_mem_usage_notifier;
    std::shared_ptr<proton::PersistenceEngine> _persistence_engine;
    std::unique_ptr<const document::FieldSetRepo> _field_set_repo;
    ServiceLayerConfigSet                      _service_layer_config;
    DistributorConfigSet                       _distributor_config;
    ConfigSet                                  _config_set;
    std::shared_ptr<config::IConfigContext>    _config_context;
    std::unique_ptr<IBmFeedHandler>            _feed_handler;
    std::unique_ptr<mbus::Slobrok>             _slobrok;
    std::shared_ptr<BmStorageLinkContext>      _service_layer_chain_context;
    std::unique_ptr<MyServiceLayerProcess>     _service_layer;
    std::shared_ptr<BmStorageLinkContext>      _distributor_chain_context;
    std::unique_ptr<storage::DistributorProcess> _distributor;

    void create_document_db(const BmClusterParams&  params);
public:
    MyBmNode(const BmClusterParams& params, std::shared_ptr<document::DocumenttypesConfig> document_types);
    ~MyBmNode() override;
    std::unique_ptr<SpiBmFeedHandler> make_create_bucket_feed_handler(bool skip_get_spi_bucket_info) override;
    void start_service_layer(const BmClusterParams& params) override;
    void wait_service_layer() override;
    void start_distributor(const BmClusterParams& params) override;
    void create_feed_handler(const BmClusterParams& params, BmCluster& cluster) override;
    void shutdown_feed_handler() override;
    void shutdown_distributor() override;
    void shutdown_service_layer() override;
    IBmFeedHandler* get_feed_handler() override;
    PersistenceProvider* get_persistence_provider() override;
};

MyBmNode::MyBmNode(const BmClusterParams& params, std::shared_ptr<document::DocumenttypesConfig> document_types)
    : BmNode(std::move(document_types)),
      _document_db_config(make_document_db_config(_document_types, _repo, _doc_type_name)),
      _base_dir(base_dir),
      _file_header_context(),
      _tls_listen_port(9017),
      _slobrok_port(9018),
      _service_layer_mbus_port(9020),
      _service_layer_rpc_port(9021),
      _service_layer_status_port(9022),
      _distributor_mbus_port(9023),
      _distributor_rpc_port(9024),
      _distributor_status_port(9025),
      _tls("tls", _tls_listen_port, _base_dir, _file_header_context),
      _tls_spec(vespalib::make_string("tcp/localhost:%d", _tls_listen_port)),
      _query_limiter(),
      _clock(),
      _metrics_wire_service(),
      _config_stores(),
      _summary_executor(8, 128_Ki),
      _document_db_owner(),
      _bucket_space(document::test::makeBucketSpace(_doc_type_name.getName())),
      _document_db(),
      _persistence_owner(),
      _write_filter(),
      _disk_mem_usage_notifier(),
      _persistence_engine(),
      _field_set_repo(std::make_unique<const document::FieldSetRepo>(*_repo)),
      _service_layer_config("bm-servicelayer", *_document_types, _slobrok_port, _service_layer_mbus_port, _service_layer_rpc_port, _service_layer_status_port, params),
      _distributor_config("bm-distributor", *_document_types, _slobrok_port, _distributor_mbus_port, _distributor_rpc_port, _distributor_status_port, params),
      _config_set(),
      _config_context(std::make_shared<config::ConfigContext>(_config_set)),
      _feed_handler(),
      _slobrok(),
      _service_layer_chain_context(),
      _service_layer(),
      _distributor_chain_context(),
      _distributor()
{
    create_document_db(params);
    _persistence_engine = std::make_unique<proton::PersistenceEngine>(_persistence_owner, _write_filter, _disk_mem_usage_notifier, -1, false);
    auto proxy = std::make_shared<proton::PersistenceHandlerProxy>(_document_db);
    _persistence_engine->putHandler(_persistence_engine->getWLock(), _bucket_space, _doc_type_name, proxy);
    _service_layer_config.add_builders(_config_set);
    _distributor_config.add_builders(_config_set);
    _feed_handler = std::make_unique<SpiBmFeedHandler>(*_persistence_engine, *_field_set_repo, params.get_skip_get_spi_bucket_info());
}

MyBmNode::~MyBmNode()
{
    if (_persistence_engine) {
        _persistence_engine->destroyIterators();
        _persistence_engine->removeHandler(_persistence_engine->getWLock(), _bucket_space, _doc_type_name);
    }
    if (_document_db) {
        _document_db->close();
    }
}


void
MyBmNode::create_document_db(const BmClusterParams& params)
{
    vespalib::mkdir(_base_dir, false);
    vespalib::mkdir(_base_dir + "/" + _doc_type_name.getName(), false);
    vespalib::string input_cfg = _base_dir + "/" + _doc_type_name.getName() + "/baseconfig";
    {
        proton::FileConfigManager fileCfg(input_cfg, "", _doc_type_name.getName());
        fileCfg.saveConfig(*_document_db_config, 1);
    }
    config::DirSpec spec(input_cfg + "/config-1");
    auto tuneFileDocDB = std::make_shared<TuneFileDocumentDB>();
    proton::DocumentDBConfigHelper mgr(spec, _doc_type_name.getName());
    auto protonCfg = std::make_shared<ProtonConfigBuilder>();
    if ( ! params.get_indexing_sequencer().empty()) {
        vespalib::string sequencer = params.get_indexing_sequencer();
        std::transform(sequencer.begin(), sequencer.end(), sequencer.begin(), [](unsigned char c){ return std::toupper(c); });
        protonCfg->indexing.optimize = ProtonConfig::Indexing::getOptimize(sequencer);
    }
    auto bootstrap_config = std::make_shared<BootstrapConfig>(1,
                                                              _document_types,
                                                              _repo,
                                                              std::move(protonCfg),
                                                              std::make_shared<FiledistributorrpcConfig>(),
                                                              std::make_shared<BucketspacesConfig>(),
                                                              tuneFileDocDB, HwInfo());
    mgr.forwardConfig(bootstrap_config);
    mgr.nextGeneration(0ms);
    _document_db = DocumentDB::create(_base_dir, mgr.getConfig(), _tls_spec, _query_limiter, _clock, _doc_type_name,
                                      _bucket_space, *bootstrap_config->getProtonConfigSP(), _document_db_owner,
                                      _summary_executor, _summary_executor, *_persistence_engine, _tls,
                                      _metrics_wire_service, _file_header_context,
                                      _config_stores.getConfigStore(_doc_type_name.toString()),
                                      std::make_shared<vespalib::ThreadStackExecutor>(16, 128_Ki), HwInfo());
    _document_db->start();
    _document_db->waitForOnlineState();
}

std::unique_ptr<SpiBmFeedHandler>
MyBmNode::make_create_bucket_feed_handler(bool skip_get_spi_bucket_info)
{
    return std::make_unique<SpiBmFeedHandler>(*_persistence_engine, *_field_set_repo, skip_get_spi_bucket_info);
}

void
MyBmNode::start_service_layer(const BmClusterParams& params)
{
    config::ConfigUri config_uri("bm-servicelayer", _config_context);
    std::unique_ptr<BmStorageChainBuilder> chain_builder;
    if (params.get_use_storage_chain() && !params.needs_distributor()) {
        chain_builder = std::make_unique<BmStorageChainBuilder>();
        _service_layer_chain_context = chain_builder->get_context();
    }
    _service_layer = std::make_unique<MyServiceLayerProcess>(config_uri,
                                                             *_persistence_engine,
                                                             std::move(chain_builder));
    _service_layer->setupConfig(100ms);
    _service_layer->createNode();
}

void
MyBmNode::wait_service_layer()
{
    _service_layer->getNode().waitUntilInitialized();
}

void
MyBmNode::start_distributor(const BmClusterParams& params)
{
    config::ConfigUri config_uri("bm-distributor", _config_context);
    std::unique_ptr<BmStorageChainBuilder> chain_builder;
    if (params.get_use_storage_chain() && !params.get_use_document_api()) {
        chain_builder = std::make_unique<BmStorageChainBuilder>();
        _distributor_chain_context = chain_builder->get_context();
    }
    _distributor = std::make_unique<storage::DistributorProcess>(config_uri);
    if (chain_builder) {
        _distributor->set_storage_chain_builder(std::move(chain_builder));
    }
    _distributor->setupConfig(100ms);
    _distributor->createNode();
}

void
MyBmNode::create_feed_handler(const BmClusterParams& params, BmCluster& cluster)
{
    StorageApiRpcService::Params rpc_params;
    // This is the same compression config as the default in stor-communicationmanager.def.
    rpc_params.compression_config = CompressionConfig(CompressionConfig::Type::LZ4, 3, 90, 1024);
    rpc_params.num_rpc_targets_per_node = params.get_rpc_targets_per_node();
    if (params.get_use_document_api()) {
        _feed_handler = std::make_unique<DocumentApiMessageBusBmFeedHandler>(cluster.get_message_bus());
    } else if (params.get_enable_distributor()) {
        if (params.get_use_storage_chain()) {
            assert(_distributor_chain_context);
            _feed_handler = std::make_unique<StorageApiChainBmFeedHandler>(_distributor_chain_context, true);
        } else if (params.get_use_message_bus()) {
            _feed_handler = std::make_unique<StorageApiMessageBusBmFeedHandler>(cluster.get_message_bus(), true);
        } else {
            _feed_handler = std::make_unique<StorageApiRpcBmFeedHandler>(cluster.get_rpc_client(), _repo, rpc_params, true);
        }
    } else if (params.needs_service_layer()) {
        if (params.get_use_storage_chain()) {
            assert(_service_layer_chain_context);
            _feed_handler = std::make_unique<StorageApiChainBmFeedHandler>(_service_layer_chain_context, false);
        } else if (params.get_use_message_bus()) {
            _feed_handler = std::make_unique<StorageApiMessageBusBmFeedHandler>(cluster.get_message_bus(), false);
        } else {
            _feed_handler = std::make_unique<StorageApiRpcBmFeedHandler>(cluster.get_rpc_client(), _repo, rpc_params, false);
        }
    }
}

void
MyBmNode::shutdown_feed_handler()
{
    _feed_handler.reset();
}

void
MyBmNode::shutdown_distributor()
{
    if (_distributor) {
        LOG(info, "stop distributor");
        _distributor->getNode().requestShutdown("controlled shutdown");
        _distributor->shutdown();
    }
}

void
MyBmNode::shutdown_service_layer()
{
    if (_service_layer) {
        LOG(info, "stop service layer");
        _service_layer->getNode().requestShutdown("controlled shutdown");
        _service_layer->shutdown();
    }
}

IBmFeedHandler*
MyBmNode::get_feed_handler()
{
    return _feed_handler.get();
}

PersistenceProvider*
MyBmNode::get_persistence_provider()
{
    return _persistence_engine.get();
}
    
std::unique_ptr<BmNode>
BmNode::create(const BmClusterParams& params, std::shared_ptr<document::DocumenttypesConfig> document_types)
{
    return std::make_unique<MyBmNode>(params, std::move(document_types));
}

}
