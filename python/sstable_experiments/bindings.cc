// sstable_experiments._native — pybind11 bridge to RocksDB internals.
//
// Pass-1 of the project ships a subprocess-only Python pipeline; this file
// scaffolds the pass-2 native extension that drives DBImpl directly, so that
// §E.3 crash-injection scenarios can SIGKILL at precise lifecycle points
// (mid-flush, mid-compaction) using SyncPoint hooks.
//
// Build: invoked by python/CMakeLists.txt — links against the rocksdb static
// library built at `../rocksdb/librocksdb.a`. Not built by `pip install -e .`
// (that is the subprocess-only path); use `pip install .[native]` instead
// once the CMake target is wired up.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/sync_point.h"

namespace py = pybind11;

namespace {

struct RunResult {
  std::uint64_t bytes_written = 0;
  std::uint64_t keys_written = 0;
  std::uint64_t recovered_keys = 0;
  std::string statistics_summary;
  int returncode = 0;
};

// Open a DB, write `num_keys` random KV pairs, close. Used as the workload
// half of §E.3 crash sub-scenarios — the caller can SIGKILL this thread (the
// native handle exposes a raise()-able pid via os.getpid()) at a chosen sync
// point and then call RecoverAndCount() to assess what survived.
RunResult FillRandom(const std::string& db_path,
                     std::uint64_t num_keys,
                     std::uint32_t value_size,
                     const std::string& sync_point_to_signal = "") {
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.statistics = rocksdb::CreateDBStatistics();

  if (!sync_point_to_signal.empty()) {
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    rocksdb::SyncPoint::GetInstance()->SetCallBack(
        sync_point_to_signal, [](void*) {
          // Python caller is expected to SIGKILL the process when this fires.
        });
  }

  rocksdb::DB* db = nullptr;
  auto s = rocksdb::DB::Open(opts, db_path, &db);
  if (!s.ok()) throw std::runtime_error("DB::Open failed: " + s.ToString());
  std::unique_ptr<rocksdb::DB> db_guard(db);

  RunResult r;
  std::string value(value_size, 'x');
  for (std::uint64_t i = 0; i < num_keys; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "key-%020llu",
                  static_cast<unsigned long long>(i));
    auto ws = db->Put(rocksdb::WriteOptions(), key, value);
    if (!ws.ok()) { r.returncode = 2; break; }
    ++r.keys_written;
  }
  r.statistics_summary = opts.statistics->ToString();
  return r;
}

// Reopen a DB and count how many keys survived a crash. The expected key
// space is `key-00000000000000000000` ... `key-<num_keys-1>` from FillRandom.
std::uint64_t RecoverAndCount(const std::string& db_path,
                              std::uint64_t expected_keys) {
  rocksdb::Options opts;
  opts.create_if_missing = false;
  rocksdb::DB* db = nullptr;
  auto s = rocksdb::DB::Open(opts, db_path, &db);
  if (!s.ok()) throw std::runtime_error("Reopen failed: " + s.ToString());
  std::unique_ptr<rocksdb::DB> db_guard(db);

  std::uint64_t found = 0;
  std::string value;
  for (std::uint64_t i = 0; i < expected_keys; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "key-%020llu",
                  static_cast<unsigned long long>(i));
    if (db->Get(rocksdb::ReadOptions(), key, &value).ok()) ++found;
  }
  return found;
}

}  // namespace

PYBIND11_MODULE(_native, m) {
  m.doc() = "sstable_experiments native bridge (pybind11)";

  py::class_<RunResult>(m, "RunResult")
      .def_readonly("bytes_written", &RunResult::bytes_written)
      .def_readonly("keys_written", &RunResult::keys_written)
      .def_readonly("recovered_keys", &RunResult::recovered_keys)
      .def_readonly("statistics_summary", &RunResult::statistics_summary)
      .def_readonly("returncode", &RunResult::returncode);

  m.def("fill_random", &FillRandom,
        py::arg("db_path"), py::arg("num_keys"),
        py::arg("value_size") = 256,
        py::arg("sync_point_to_signal") = "",
        "Open db_path, write num_keys random KVs, close.");

  m.def("recover_and_count", &RecoverAndCount,
        py::arg("db_path"), py::arg("expected_keys"),
        "Reopen db_path and report how many keys are readable.");
}
